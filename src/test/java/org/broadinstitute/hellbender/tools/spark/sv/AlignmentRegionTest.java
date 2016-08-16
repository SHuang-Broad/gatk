package org.broadinstitute.hellbender.tools.spark.sv;

import htsjdk.samtools.TextCigarCodec;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class AlignmentRegionTest {

    @Test
    public void testAlignmentRegionOverlap() throws Exception {

        //overlap by 1
        final AlignmentRegion ar1 = new AlignmentRegion("1","1", TextCigarCodec.decode("5M5H"),true,new SimpleInterval("1",1,5),60,1,5,0);
        final AlignmentRegion ar2 = new AlignmentRegion("1","1", TextCigarCodec.decode("5S5M"),true,new SimpleInterval("1",10,16),60,5,10,0);
        Assert.assertEquals(ar1.overlapOnContig(ar2), 1);

        // don't overlap
        final AlignmentRegion ar3 = new AlignmentRegion("1","1", TextCigarCodec.decode("5M5H"),true,new SimpleInterval("1",1,5),60,1,5,0);
        final AlignmentRegion ar4 = new AlignmentRegion("1","1", TextCigarCodec.decode("5S5M"),true,new SimpleInterval("1",11,16),60,6,10,0);
        Assert.assertEquals(ar3.overlapOnContig(ar4), 0);

    }

}