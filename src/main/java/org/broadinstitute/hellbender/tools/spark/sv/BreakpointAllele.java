package org.broadinstitute.hellbender.tools.spark.sv;

import org.broadinstitute.hellbender.utils.SimpleInterval;

import java.util.Objects;

/**
 * This class represents the allele of an SV breakpoint (a novel adjacency between two genomic locations
 */
class BreakpointAllele {
    final SimpleInterval leftAlignedLeftBreakpoint;
    final SimpleInterval leftAlignedRightBreakpoint;
    final String insertedSequence;
    final String homology;
    final boolean fiveToThree;
    final boolean threeToFive;

    public BreakpointAllele(final SimpleInterval leftAlignedLeftBreakpoint, final SimpleInterval leftAlignedRightBreakpoint, final String insertedSequence, final String homology, final boolean fiveToThree, final boolean threeToFive) {
        this.leftAlignedLeftBreakpoint = leftAlignedLeftBreakpoint;
        this.leftAlignedRightBreakpoint = leftAlignedRightBreakpoint;
        this.insertedSequence = insertedSequence;
        this.homology = homology;
        this.fiveToThree = fiveToThree;
        this.threeToFive = threeToFive;
    }

    public enum InversionType{
        INV_3_TO_5, INV_5_TO_3, INV_UNKNOWN
    }

    public InversionType getInversionType(){
        if(!fiveToThree && threeToFive){
            return InversionType.INV_3_TO_5;
        }else if(fiveToThree && !threeToFive){
            return InversionType.INV_5_TO_3;
        }
        return InversionType.INV_UNKNOWN;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final BreakpointAllele that = (BreakpointAllele) o;
        return fiveToThree == that.fiveToThree &&
                threeToFive == that.threeToFive &&
                Objects.equals(leftAlignedLeftBreakpoint, that.leftAlignedLeftBreakpoint) &&
                Objects.equals(leftAlignedRightBreakpoint, that.leftAlignedRightBreakpoint) &&
                Objects.equals(insertedSequence, that.insertedSequence) &&
                Objects.equals(homology, that.homology);
    }

    @Override
    public int hashCode() {
        return Objects.hash(leftAlignedLeftBreakpoint, leftAlignedRightBreakpoint, insertedSequence, homology, fiveToThree, threeToFive);
    }
}
