package org.broadinstitute.hellbender.tools.spark.sv;

import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.common.annotations.VisibleForTesting;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.util.SequenceUtil;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import htsjdk.variant.variantcontext.writer.Options;
import htsjdk.variant.variantcontext.writer.VariantContextWriter;
import htsjdk.variant.variantcontext.writer.VariantContextWriterBuilder;
import htsjdk.variant.vcf.VCFConstants;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFStandardHeaderLines;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.programgroups.StructuralVariationSparkProgramGroup;
import org.broadinstitute.hellbender.engine.datasources.ReferenceMultiSource;
import org.broadinstitute.hellbender.engine.datasources.ReferenceWindowFunctions;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.utils.IntervalUtils;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.gcs.BucketUtils;
import scala.Tuple2;
import scala.Tuple4;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.broadinstitute.hellbender.tools.spark.sv.ContigsCollection.loadContigsCollectionKeyedByAssemblyId;

@CommandLineProgramProperties(summary="Filter breakpoint alignments and call variants.",
        oneLineSummary="Filter breakpoint alignments and call variants",
        programGroup = StructuralVariationSparkProgramGroup.class)
public final class CallVariantsFromAlignedContigsSpark extends GATKSparkTool {
    private static final long serialVersionUID = 1L;
    public static final Integer DEFAULT_MIN_ALIGNMENT_LENGTH = 50;
    private static final Logger log = LogManager.getLogger(CallVariantsFromAlignedContigsSpark.class);

    @Argument(doc = "URI of the output path", shortName = "outputPath",
            fullName = "outputPath", optional = false)
    private String outputPath;

    @Argument(doc = "Input file of contig alignments", shortName = "inputAlignments",
            fullName = "inputAlignments", optional = false)
    private String inputAlignments;

    @Argument(doc = "Input file of assembled contigs", shortName = "inputAssemblies",
            fullName = "inputAssemblies", optional = false)
    private String inputAssemblies;

    @Argument(doc = "Minimum flanking alignment length", shortName = "minAlignLength",
            fullName = "minAlignLength", optional = true)
    private Integer minAlignLength = CallVariantsFromAlignedContigsSpark.DEFAULT_MIN_ALIGNMENT_LENGTH;

    // This class requires a reference parameter in 2bit format (to broadcast) and a reference in FASTA format
    // (to get a good sequence dictionary).
    // todo: document this better
    @Argument(doc = "FASTA formatted reference", shortName = "fastaReference",
            fullName = "fastaReference", optional = false)
    private String fastaReference;

    @Override
    public boolean requiresReference() {
        return true;
    }

    @Override
    protected void runTool(final JavaSparkContext ctx) {
//        final Broadcast<ReferenceMultiSource> broadcastReference = ctx.broadcast(getReference());
//        final Integer minAlignLengthFinal = this.minAlignLength;

        final JavaPairRDD<Tuple2<String, String>, Tuple2<Iterable<AlignmentRegion>, byte[]>> alignmentRegionsWithContigSequences = prepAlignmentRegionsForCalling(ctx);

        callVariantsFromAlignmentRegionsAndWriteVariants(ctx.broadcast(getReference()), alignmentRegionsWithContigSequences, this.minAlignLength, fastaReference, getAuthenticatedGCSOptions(), outputPath);
    }

    private JavaPairRDD<Tuple2<String, String>, Tuple2<Iterable<AlignmentRegion>, byte[]>> prepAlignmentRegionsForCalling(JavaSparkContext ctx) {
        final JavaRDD<AlignmentRegion> inputAlignedContigs = ctx.textFile(inputAlignments).map(ContigsCollection::parseAlignedAssembledContigLine);

        final JavaPairRDD<Tuple2<String, String>, Iterable<AlignmentRegion>> alignmentRegionsKeyedByBreakpointAndContig = inputAlignedContigs.mapToPair(alignmentRegion -> new Tuple2<>(new Tuple2<>(alignmentRegion.assemblyId, alignmentRegion.contigId), alignmentRegion)).groupByKey();

        final JavaPairRDD<String, ContigsCollection> assemblyIdsToContigCollections = loadContigsCollectionKeyedByAssemblyId(ctx, inputAssemblies).cache();

        final JavaPairRDD<Tuple2<String, String>, byte[]> contigSequences = assemblyIdsToContigCollections.flatMapToPair(assemblyIdAndContigsCollection -> {
            final String assemblyId = assemblyIdAndContigsCollection._1;
            final ContigsCollection contigsCollection = assemblyIdAndContigsCollection._2;
            return contigsCollection.getContents().stream().map(pair -> new Tuple2<>(new Tuple2<>(assemblyId, pair._1.toString()), pair._2.toString().getBytes())).collect(Collectors.toList());
        });

        return alignmentRegionsKeyedByBreakpointAndContig.join(contigSequences);
    }

    /**
     * This method processes an RDD containing alignment regions, scanning for split alignments which match a set of filtering
     * criteria, and emitting a list of VariantContexts representing SVs for split alignments that pass. It then writes the variants
     * to the output path.
     *
     * The input RDD is of the form:
     *
     * Key: Tuple2 of two Strings: the assembly ID and the contig ID that the alignments come from
     * Value: Tuple2 of:
     *     {@code Iterable<AlignmentRegion>} AlignmentRegion objects representing all alignments for the contig
     *     A byte array with the sequence content of the contig
     *
     * FASTA and Broadcast references are both required because 2bit Broadcast references currently order their
     * sequence dictionaries in a scrambled order, see https://github.com/broadinstitute/gatk/issues/2037.
     *
     * @param broadcastReference The broadcast handle to the reference (used to populate reference bases)
     * @param alignmentRegionsWithContigSequences A data structure as described above, where a list of AlignmentRegions and the sequence of the contig are keyed by a tuple of Assembly ID and Contig ID
     * @param minAlignLength The minimum length of alignment regions flanking the breakpoint to emit an SV variant
     * @param fastaReference The reference in FASTA format, used to get a sequence dictionary and sort the variants according to it
     * @param pipelineOptions GCS pipeline option for creating and writing output files
     * @param outputPath Path to write the output files
     * @return An RDD of VariantContexts representing SVs called from breakpoint alignments
     */
    protected static void callVariantsFromAlignmentRegionsAndWriteVariants(final Broadcast<ReferenceMultiSource> broadcastReference,
                                                                           final JavaPairRDD<Tuple2<String, String>, Tuple2<Iterable<AlignmentRegion>, byte[]>> alignmentRegionsWithContigSequences,
                                                                           final Integer minAlignLength,
                                                                           final String fastaReference,
                                                                           final PipelineOptions pipelineOptions,
                                                                           final String outputPath) {

        final JavaRDD<VariantContext> variantContextJavaRDD = callVariantsFromAlignmentRegions(broadcastReference, alignmentRegionsWithContigSequences, minAlignLength);

        final List<VariantContext> variants = variantContextJavaRDD.collect();
        log.info("Called " + variants.size() + " inversions");

        final SAMSequenceDictionary referenceSequenceDictionary = new ReferenceMultiSource(pipelineOptions, fastaReference, ReferenceWindowFunctions.IDENTITY_FUNCTION).getReferenceSequenceDictionary(null);

        final List<VariantContext> sortedVariantsList = new ArrayList<>(variants);
        sortedVariantsList.sort((VariantContext v1, VariantContext v2) -> IntervalUtils.compareLocatables(v1, v2, referenceSequenceDictionary));

        writeVariants(outputPath, sortedVariantsList, pipelineOptions, "inversions.vcf", referenceSequenceDictionary);
    }
    ////////////////////////////
    /**
     * This method processes an RDD containing alignment regions, scanning for split alignments which match a set of filtering
     * criteria, and emitting a list of VariantContexts representing SVs for split alignments that pass.
     *
     * The input RDD is of the form:
     *
     * Key: Tuple2 of two Strings: the assembly ID and the contig ID that the alignments come from
     * Value: Tuple2 of:
     *     {@code Iterable<AlignmentRegion>} AlignmentRegion objects representing all alignments for the contig
     *     A byte array with the sequence content of the contig
     *
     * @param broadcastReference The broadcast handle to the reference (used to populate reference bases)
     * @param alignmentRegionsWithContigSequences A data structure as described above, where a list of AlignmentRegions and the sequence of the contig are keyed by a tuple of Assembly ID and Contig ID
     * @param minAlignLength The minimum length of alignment regions flanking the breakpoint to emit an SV variant
     * @return An RDD of VariantContexts representing SVs called from breakpoint alignments
     */
    private static JavaRDD<VariantContext> callVariantsFromAlignmentRegions(final Broadcast<ReferenceMultiSource> broadcastReference,
                                                                            final JavaPairRDD<Tuple2<String, String>, Tuple2<Iterable<AlignmentRegion>, byte[]>> alignmentRegionsWithContigSequences,
                                                                            final Integer minAlignLength) {

        // first construct breakpoint alignments from alignment regions of each contig
        JavaPairRDD<Tuple2<String, String>, BreakpointAlignment> assembledBreakpointsByAssemblyIdAndContigId =
                alignmentRegionsWithContigSequences.flatMapValues(alignmentRegionsAndSequences ->
                                CallVariantsFromAlignedContigsSpark.getBreakpointAlignmentsFromAlignmentRegions(alignmentRegionsAndSequences._2,
                                        StreamSupport.stream(alignmentRegionsAndSequences._1.spliterator(), false).sorted(Comparator.comparing(a -> a.startInAssembledContig)).collect(Collectors.toList()),
                                        minAlignLength));

        // second turn breakpoint alignment into consensus breakpoint alleles
        final JavaPairRDD<BreakpointAllele, Tuple2<Tuple2<String,String>, BreakpointAlignment>> assembled3To5BreakpointsKeyedByPosition =
                assembledBreakpointsByAssemblyIdAndContigId
                        .mapToPair(CallVariantsFromAlignedContigsSpark::constructBreakpointAlleleFromBreakpointAlignment)
                        .filter(CallVariantsFromAlignedContigsSpark::breakpointAlleleRepresentsInversion);
        final JavaPairRDD<BreakpointAllele, Iterable<Tuple2<Tuple2<String,String>, BreakpointAlignment>>> groupedBreakpoints = assembled3To5BreakpointsKeyedByPosition.groupByKey();

        // finally collect the original alignment information that made the call and create variant
        return groupedBreakpoints.map(breakpoints -> collectAlignmentInfoAndProduceVariant(breakpoints, broadcastReference));
    }

    //////////////////////////// important stuff

    /**
     * Assumes the input {@code alignmentRegionList} is sorted by start position in assembled contig
     * but DOES NOT assume all AR's map to the same chromosome.
     * The same-contig assumption is enforced later by {@link #breakpointAlleleRepresentsInversion(Tuple2)}.
     * @param sequence
     * @param alignmentRegionList
     * @param minAlignLength
     * @return
     */
    static List<BreakpointAlignment> getBreakpointAlignmentsFromAlignmentRegions(final byte[] sequence,
                                                                                 final List<AlignmentRegion> alignmentRegionList,
                                                                                 final Integer minAlignLength) {
        if (alignmentRegionList.isEmpty()) {
            return new ArrayList<>();
        }

        final List<BreakpointAlignment> results = new ArrayList<>(alignmentRegionList.size() - 1);
        final List<String> insertionAlignmentRegions = new ArrayList<>();

        final Iterator<AlignmentRegion> iterator = alignmentRegionList.iterator();
        AlignmentRegion current = iterator.next(); // guaranteed by the if check at the beginning
        while (treatAlignmentRegionAsInsertion(current) && iterator.hasNext()) { // skip those that can be treated as insertion
            current = iterator.next();
        }
        while ( iterator.hasNext() ) {
            final AlignmentRegion next = iterator.next();                       // always treat AR in pair (two ARs make one breakpoint alignment)
            if (currentAlignmentRegionIsTooSmall(current, next, minAlignLength)) { // skip one that's too small
                continue;
            }

            if (treatNextAlignmentRegionInPairAsInsertion(current, next, minAlignLength)) { // if, for this pair, the next AR can be treated as insertion
                if (iterator.hasNext()) {                                                   //   and if there's still another one after next
                    insertionAlignmentRegions.add(next.toPackedString());                   //   then add the next one to a container then move on
                    continue;
                } else {                                                                    //   otherwise break out of the whole loop
                    break;
                }
            }

            final byte[] sequenceCopy = Arrays.copyOf(sequence, sequence.length); // why extra copy here?

            String homology = getHomology(next, current, sequenceCopy);
            String insertedSequence = getInsertedSequence(next, current, sequenceCopy);

            results.add(new BreakpointAlignment(next.contigId, current, next, insertedSequence, homology, insertionAlignmentRegions));

            current = next;
        }
        return results;
    }

    /**
     * TODO: what is the filter's logic here?
     * @param next
     * @return
     */
    private static boolean treatAlignmentRegionAsInsertion(final AlignmentRegion next) {
        return next.mapqual < 60;
    }

    /**
     * Test if current alignment region, taking out its overlap with the next, is strictly less than a threshold.
     */
    private static boolean currentAlignmentRegionIsTooSmall(final AlignmentRegion current, final AlignmentRegion next, final Integer minAlignLength) {
        return current.referenceInterval.size() - current.overlapOnContig(next) < minAlignLength;
    }

    /**
     * TODO: what's the logic here?
     * True if any of these:    1) next alignment region can be treated as insertion
     *                          2) next alignment region is too small
     *                          3) either of regions contains the other
     */
    @VisibleForTesting
    static boolean treatNextAlignmentRegionInPairAsInsertion(AlignmentRegion current, AlignmentRegion next, final Integer minAlignLength) {
        return treatAlignmentRegionAsInsertion(next) ||
                (next.referenceInterval.size() - current.overlapOnContig(next) < minAlignLength) || // TODO: could this be replaced with nextAlignmentRegionIsTooSmall()?
                current.referenceInterval.contains(next.referenceInterval) ||
                next.referenceInterval.contains(current.referenceInterval);
    }

    private static String getHomology(final AlignmentRegion current,
                                      final AlignmentRegion previous,
                                      final byte[] sequenceCopy) {

        if (previous.endInAssembledContig >= current.startInAssembledContig) { // if the two AR's overlap on contig
            final byte[] homologyBytes = Arrays.copyOfRange(sequenceCopy, current.startInAssembledContig - 1, previous.endInAssembledContig);
            if (previous.referenceInterval.getStart() > current.referenceInterval.getStart()) {
                SequenceUtil.reverseComplement(homologyBytes, 0, homologyBytes.length);
            }
            return new String(homologyBytes);
        } else {
            return "";
        }
    }

    private static String getInsertedSequence(final AlignmentRegion current,
                                              final AlignmentRegion previous,
                                              final byte[] sequenceCopy) {

        if (previous.endInAssembledContig < current.startInAssembledContig - 1) { // if the two AR's boundary doesn't touch

            final int insertionStart = previous.endInAssembledContig + 1;
            final int insertionEnd = current.startInAssembledContig - 1;

            final byte[] insertedSequenceBytes = Arrays.copyOfRange(sequenceCopy, insertionStart - 1, insertionEnd);
            if (previous.referenceInterval.getStart() > current.referenceInterval.getStart()) {
                SequenceUtil.reverseComplement(insertedSequenceBytes, 0, insertedSequenceBytes.length);
            }
            return new String(insertedSequenceBytes);
        } else {
            return "";
        }
    }

    /////////////////////////

    @VisibleForTesting
    static Tuple2<BreakpointAllele, Tuple2<Tuple2<String,String>, BreakpointAlignment>> constructBreakpointAlleleFromBreakpointAlignment(final Tuple2<Tuple2<String, String>, BreakpointAlignment> breakpointIdAndAssembledBreakpoint) {
        return new Tuple2<>(BreakpointAllele.fromBreakpointAlignment(breakpointIdAndAssembledBreakpoint._2), breakpointIdAndAssembledBreakpoint);
    }

    /**
     * Simple filter that the two breakpoints share the same contig, and is not a INV_NONE, i.e. it is really an inversion.
     */
    @VisibleForTesting
    static Boolean breakpointAlleleRepresentsInversion(final Tuple2<BreakpointAllele, Tuple2<Tuple2<String, String>, BreakpointAlignment>> breakpointAlleleTuple2Tuple2) {
        final BreakpointAllele breakpointAllele = breakpointAlleleTuple2Tuple2._1;
        return breakpointAllele.leftAlignedLeftBreakpoint.getContig().equals(breakpointAllele.leftAlignedRightBreakpoint.getContig()) && (breakpointAllele.inversionType != BreakpointAllele.InversionType.INV_NONE);
    }

    //////////////////////////

    /**
     * No filter actually happen here, instead the alignment information stored in BreakpointAlignment's for one BreakpointAllele
     * is collected and used for produce an VariantContext.
     * @param assembledBreakpointsPerAllele
     * @param broadcastReference
     * @return
     * @throws IOException
     */
    @VisibleForTesting
    private static VariantContext collectAlignmentInfoAndProduceVariant(final Tuple2<BreakpointAllele, Iterable<Tuple2<Tuple2<String, String>, BreakpointAlignment>>> assembledBreakpointsPerAllele,
                                                                        final Broadcast<ReferenceMultiSource> broadcastReference)
            throws IOException {

        // MQ, alignLen, asmID, contigID
        final List<Tuple4<Integer, Integer, String, String>> breakpointAlignmentInformation = StreamSupport.stream(assembledBreakpointsPerAllele._2.spliterator(), false).map(entry ->{
            final BreakpointAlignment breakpointAlignment = entry._2();
            final int assembledBreakpointAlignmentLength =
                    Math.min(breakpointAlignment.region1.referenceInterval.size(),
                            breakpointAlignment.region2.referenceInterval.size()) - breakpointAlignment.region1.overlapOnContig(breakpointAlignment.region2);
            return new Tuple4<>(Math.min(breakpointAlignment.region1.mapqual, breakpointAlignment.region2.mapqual), // mqs
                    assembledBreakpointAlignmentLength, // alignLengths
                    entry._1._1, //assemblyID
                    entry._1._2);//contigID
        }).collect(Collectors.toList());

        return createVariant(breakpointAlignmentInformation, assembledBreakpointsPerAllele._1, broadcastReference.getValue());
    }

    @VisibleForTesting
    private static VariantContext createVariant(final List<Tuple4<Integer, Integer, String, String>> breakpointAlignmentInformation,
                                                final BreakpointAllele breakpointAllele,
                                                final ReferenceMultiSource reference)
            throws IOException{

        final String contig = breakpointAllele.leftAlignedLeftBreakpoint.getContig();
        final int start = breakpointAllele.leftAlignedLeftBreakpoint.getStart();
        final int end = breakpointAllele.leftAlignedRightBreakpoint.getStart();

        final List<Allele> vcAlleles = Arrays.asList(Allele.create(new String(reference.getReferenceBases(null, new SimpleInterval(contig, start, start)).getBases()), true),
                                                     Allele.create("<INV>"));

        final int maxAlignLength = breakpointAlignmentInformation.stream().map(e -> e._2()).max(Comparator.naturalOrder()).orElse(0);
        final int highMqMappings = (int) breakpointAlignmentInformation.stream().map(e -> e._1()).filter(i -> i==60).count();

        VariantContextBuilder vcBuilder = new VariantContextBuilder()
                .chr(contig)
                .start(start)
                .stop(end)
                .id(getInversionId(breakpointAllele))
                .alleles(vcAlleles)
                .attribute(VCFConstants.END_KEY, end)
                .attribute(GATKSVVCFHeaderLines.SVTYPE, GATKSVVCFHeaderLines.SVTYPES.INV.toString())
                .attribute(GATKSVVCFHeaderLines.SVLEN, end - start)
                .attribute(GATKSVVCFHeaderLines.TOTAL_MAPPINGS, breakpointAlignmentInformation.size())
                .attribute(GATKSVVCFHeaderLines.HQ_MAPPINGS, highMqMappings)
                .attribute(GATKSVVCFHeaderLines.MAX_ALIGN_LENGTH, maxAlignLength)
                .attribute(GATKSVVCFHeaderLines.MAPPING_QUALITIES, breakpointAlignmentInformation.stream().map(e -> e._1()).map(String::valueOf).collect(Collectors.joining(",")))
                .attribute(GATKSVVCFHeaderLines.ALIGN_LENGTHS, breakpointAlignmentInformation.stream().map(e -> e._2()).map(String::valueOf).collect(Collectors.joining(",")))
                .attribute(GATKSVVCFHeaderLines.ASSEMBLY_IDS, breakpointAlignmentInformation.stream().map(e -> e._3()).collect(Collectors.joining(",")))
                .attribute(GATKSVVCFHeaderLines.CONTIG_IDS, breakpointAlignmentInformation.stream().map(e -> e._4()).map(s -> s.replace(" ", "_")).collect(Collectors.joining(",")));

        if (!breakpointAllele.insertedSequence.isEmpty()) {
            vcBuilder = vcBuilder.attribute(GATKSVVCFHeaderLines.INSERTED_SEQUENCE, breakpointAllele.insertedSequence);
        }

        if (!breakpointAllele.insertionMappings.isEmpty()) {
            vcBuilder = vcBuilder.attribute(GATKSVVCFHeaderLines.INSERTED_SEQUENCE_MAPPINGS, breakpointAllele.insertionMappings.stream().collect(Collectors.joining(";")));
        }

        if (!breakpointAllele.homology.isEmpty()) {
            vcBuilder = vcBuilder.attribute(GATKSVVCFHeaderLines.HOMOLOGY, breakpointAllele.homology);
            vcBuilder = vcBuilder.attribute(GATKSVVCFHeaderLines.HOMOLOGY_LENGTH, breakpointAllele.homology.length());
        }

        if (breakpointAllele.inversionType == BreakpointAllele.InversionType.INV_5_TO_3) {
            vcBuilder = vcBuilder.attribute(GATKSVVCFHeaderLines.INV_5_TO_3, "");
        }

        if (breakpointAllele.inversionType == BreakpointAllele.InversionType.INV_3_TO_5) {
            vcBuilder = vcBuilder.attribute(GATKSVVCFHeaderLines.INV_3_TO_5, "");
        }

        return vcBuilder.make();
    }

    private static String getInversionId(final BreakpointAllele breakpointAllele) {
        final String invType = breakpointAllele.inversionType.name();
        return invType + "_" + breakpointAllele.leftAlignedLeftBreakpoint.getContig() + "_" + breakpointAllele.leftAlignedLeftBreakpoint.getStart() + "_" + breakpointAllele.leftAlignedRightBreakpoint.getStart();
    }

    ////////////////////////////

    private static void writeVariants(final String outputPath, final List<VariantContext> variantsArrayList, final PipelineOptions pipelineOptions, final String fileName, final SAMSequenceDictionary referenceSequenceDictionary) {

        try (final OutputStream outputStream = new BufferedOutputStream(
                BucketUtils.createFile(outputPath + "/" + fileName, pipelineOptions))) {

            final VCFHeader header = getVcfHeader(referenceSequenceDictionary);
            final VariantContextWriter vcfWriter = getVariantContextWriter(outputStream, referenceSequenceDictionary);
            vcfWriter.writeHeader(header);
            variantsArrayList.forEach(vcfWriter::add);
            vcfWriter.close();
        } catch (IOException e) {
            throw new GATKException("Could not create output file", e);
        }
    }

    private static VCFHeader getVcfHeader(final SAMSequenceDictionary referenceSequenceDictionary) {
        final VCFHeader header = new VCFHeader();
        header.setSequenceDictionary(referenceSequenceDictionary);
        header.addMetaDataLine(VCFStandardHeaderLines.getInfoLine(VCFConstants.END_KEY));
        GATKSVVCFHeaderLines.vcfHeaderLines.values().forEach(header::addMetaDataLine);
        return header;
    }

    private static VariantContextWriter getVariantContextWriter(final OutputStream outputStream, final SAMSequenceDictionary referenceSequenceDictionary) {
        VariantContextWriterBuilder vcWriterBuilder = new VariantContextWriterBuilder()
                .clearOptions()
                .setOutputStream(outputStream);

        if (null != referenceSequenceDictionary) {
            vcWriterBuilder = vcWriterBuilder.setReferenceDictionary(referenceSequenceDictionary);
        }
        // todo: remove this when things are solid?
        vcWriterBuilder = vcWriterBuilder.setOption(Options.ALLOW_MISSING_FIELDS_IN_HEADER);
        for (Options opt : new Options[]{}) {
            vcWriterBuilder = vcWriterBuilder.setOption(opt);
        }

        return vcWriterBuilder.build();
    }
}
