package org.broadinstitute.hellbender.tools.spark.sv;

import org.broadinstitute.hellbender.utils.SimpleInterval;
import scala.Tuple2;

import java.util.List;
import java.util.Objects;

/**
 * This class represents the allele of an SV breakpoint (a novel adjacency between two genomic locations
 */
class BreakpointAllele {
    final SimpleInterval leftAlignedLeftBreakpoint;
    final SimpleInterval leftAlignedRightBreakpoint;
    final String insertedSequence;
    final String homology;
    final InversionType inversionType;

    // not included in equals and hashCode so as not to break grouping by breakpoint allele if the mappings are different
    final List<String> insertionMappings;

    /**
     * Returns the canonical representation of the breakpoint implied by this split contig alignment,
     * including whether it is a 3-5 or 5-3 inversion, and the homology and inserted sequence at the
     * breakpoint. The two intervals returned are 1bp intervals indicating the exact breakpoint
     * location. If there is homology at the breakpoint, the breakpoint locations will be left
     * aligned.
     * @return
     */
    public static BreakpointAllele fromBreakpointAlignment(final BreakpointAlignment breakpointAlignment) {

        final Tuple2<SimpleInterval, SimpleInterval> bps = breakpointAlignment.get5And3BPsLeftAlignedOnContig();
        final SimpleInterval fiveEndBPLeftAlignedOnContig = bps._1();
        final SimpleInterval threeEndBPLeftAlignedOnContig = bps._2();

        final boolean isFiveToThreeInversion;
        final boolean isThreeToFiveInversion;

        final AlignmentRegion baRegion1 = breakpointAlignment.region1;
        final AlignmentRegion baRegion2 = breakpointAlignment.region2;
        final String baHomology = breakpointAlignment.homology;
        final String baInsertedSeq = breakpointAlignment.insertedSequence;
        final List<String> baInsertionMappings = breakpointAlignment.insertionMappings;

        if (baRegion1.referenceInterval.getStart() < baRegion2.referenceInterval.getStart()) {
            isFiveToThreeInversion = baRegion1.forwardStrand && !baRegion2.forwardStrand;
            isThreeToFiveInversion = !baRegion1.forwardStrand && baRegion2.forwardStrand;
        } else {
            isFiveToThreeInversion = !baRegion1.forwardStrand && baRegion2.forwardStrand;
            isThreeToFiveInversion = baRegion1.forwardStrand && !baRegion2.forwardStrand;
        }

        if (!fiveEndBPLeftAlignedOnContig.getContig().equals(threeEndBPLeftAlignedOnContig.getContig())) {
            return new BreakpointAllele(fiveEndBPLeftAlignedOnContig, threeEndBPLeftAlignedOnContig, baInsertedSeq, baHomology, isFiveToThreeInversion, isThreeToFiveInversion, baInsertionMappings);
        } else if (fiveEndBPLeftAlignedOnContig.getStart() < threeEndBPLeftAlignedOnContig.getStart()) {
            return new BreakpointAllele(fiveEndBPLeftAlignedOnContig, threeEndBPLeftAlignedOnContig, baInsertedSeq, baHomology, isFiveToThreeInversion, isThreeToFiveInversion, baInsertionMappings);
        } else {
            return new BreakpointAllele(threeEndBPLeftAlignedOnContig, fiveEndBPLeftAlignedOnContig, baInsertedSeq, baHomology, isFiveToThreeInversion, isThreeToFiveInversion, baInsertionMappings);
        }
    }

    protected BreakpointAllele(final SimpleInterval leftAlignedLeftBreakpoint,
                               final SimpleInterval leftAlignedRightBreakpoint,
                               final String insertedSequence,
                               final String homology,
                               final boolean fiveToThree,
                               final boolean threeToFive,
                               final List<String> insertionMappings) {
        this.leftAlignedLeftBreakpoint = leftAlignedLeftBreakpoint;
        this.leftAlignedRightBreakpoint = leftAlignedRightBreakpoint;
        this.insertedSequence = insertedSequence;
        this.homology = homology;
        inversionType = getInversionType(fiveToThree, threeToFive);
        this.insertionMappings = insertionMappings;
    }

    public enum InversionType{
        INV_3_TO_5, INV_5_TO_3, INV_NONE
    }

    private InversionType getInversionType(final boolean fiveToThree, final boolean threeToFive){
        if(!fiveToThree && threeToFive){
            return InversionType.INV_3_TO_5;
        }else if(fiveToThree && !threeToFive){
            return InversionType.INV_5_TO_3;
        }
        return InversionType.INV_NONE;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final BreakpointAllele that = (BreakpointAllele) o;
        return inversionType == that.inversionType &&
                Objects.equals(leftAlignedLeftBreakpoint, that.leftAlignedLeftBreakpoint) &&
                Objects.equals(leftAlignedRightBreakpoint, that.leftAlignedRightBreakpoint) &&
                Objects.equals(insertedSequence, that.insertedSequence) &&
                Objects.equals(homology, that.homology);
    }

    @Override
    public int hashCode() {
        return Objects.hash(leftAlignedLeftBreakpoint, leftAlignedRightBreakpoint, insertedSequence, homology, inversionType);
    }
}
