package org.NaiveBayes;

public class ModelData {
    private double prior;
    private double[] means;
    private double[] variances;

    public ModelData(double prior, double[] means, double[] variances) {
        this.prior = prior;
        this.means = means;
        this.variances = variances;
    }

    public double getPrior() {
        return prior;
    }

    public double[] getMeans() {
        return means;
    }

    public double[] getVariances() {
        return variances;
    }
}

