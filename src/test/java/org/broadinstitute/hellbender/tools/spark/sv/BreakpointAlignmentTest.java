package org.broadinstitute.hellbender.tools.spark.sv;

import htsjdk.samtools.TextCigarCodec;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;

public class BreakpointAlignmentTest {

    @Test
    public void testGetLeftAlignedLeftBreakpointOnAssembledContig() throws Exception {
        final AlignmentRegion region1 = new AlignmentRegion("1","1", TextCigarCodec.decode("100M100S"), true, new SimpleInterval("1", 100, 200), 60, 1, 100, 0);
        final AlignmentRegion region2 = new AlignmentRegion("1","1", TextCigarCodec.decode("100M100S"), false, new SimpleInterval("1", 500, 600), 60, 101, 200, 0);
        final BreakpointAlignment breakpointAlignment = new BreakpointAlignment("1", region1, region2, "", "", new ArrayList<>());
        Assert.assertEquals(breakpointAlignment.getLeftAlignedLeftBreakpointOnAssembledContig(), new SimpleInterval("1", 200, 200));
    }

    @Test
    public void testGetLeftAlignedLeftBreakpointOnAssembledContigWithHomology() throws Exception {
        final AlignmentRegion region1 = new AlignmentRegion("1","1", TextCigarCodec.decode("105M100S"), true, new SimpleInterval("1", 100, 205), 60, 1, 105, 0);
        final AlignmentRegion region2 = new AlignmentRegion("1","1", TextCigarCodec.decode("105M100S"), false, new SimpleInterval("1", 500, 605), 60, 95, 200, 0);
        final BreakpointAlignment breakpointAlignment = new BreakpointAlignment("1", region1, region2, "", "ACACA", new ArrayList<>());
        Assert.assertEquals(breakpointAlignment.getLeftAlignedLeftBreakpointOnAssembledContig(), new SimpleInterval("1", 200, 200));
    }

    @Test
    public void testGetBreakpointAllele() throws Exception {
        final AlignmentRegion region1 = new AlignmentRegion("1","1", TextCigarCodec.decode("105M100S"), true, new SimpleInterval("1", 100, 205), 60, 1, 105, 0);
        final AlignmentRegion region2 = new AlignmentRegion("1","1", TextCigarCodec.decode("100S105M"), false, new SimpleInterval("1", 505, 605), 60, 95, 200, 0);
        final BreakpointAlignment breakpointAlignment = new BreakpointAlignment("1", region1, region2, "", "ACACA", new ArrayList<>());
        final BreakpointAllele breakpointAllele = breakpointAlignment.getBreakpointAllele();
        Assert.assertEquals(breakpointAllele.leftAlignedLeftBreakpoint, new SimpleInterval("1", 200, 200));
        Assert.assertEquals(breakpointAllele.leftAlignedRightBreakpoint, new SimpleInterval("1", 605, 605));
        Assert.assertTrue(breakpointAllele.fiveToThree);
        Assert.assertFalse(breakpointAllele.threeToFive);
        Assert.assertEquals(breakpointAllele.homology, "ACACA");
    }

    @Test
    public void testAlignedBreakpointBreakpointAlelle() throws Exception {
        final AlignmentRegion region1 = new AlignmentRegion("1", "contig-1", TextCigarCodec.decode("146M51S"), true, new SimpleInterval("8", 108569148, 108569294), 60, 1, 146, 0);
        final AlignmentRegion region2 = new AlignmentRegion("1", "contig-1", TextCigarCodec.decode("147S50M"), false, new SimpleInterval("8", 108569314, 108569364), 60, 148, 197, 0);
        final BreakpointAlignment breakpointAlignment = new BreakpointAlignment("contig-1", region1, region2, "TC", "", new ArrayList<>());
        final SimpleInterval leftAlignedLeftBreakpointOnAssembledContig = breakpointAlignment.getLeftAlignedLeftBreakpointOnAssembledContig();
        Assert.assertEquals(leftAlignedLeftBreakpointOnAssembledContig, new SimpleInterval("8", 108569294, 108569294));
        final SimpleInterval leftAlignedRightBreakpointOnAssembledContig = breakpointAlignment.getLeftAlignedRightBreakpointOnAssembledContig();
        Assert.assertEquals(leftAlignedRightBreakpointOnAssembledContig, new SimpleInterval("8", 108569364, 108569364));
    }

}