package org.apache.tinkerpop.gremlin.driver;

class EndpointConnectionMetrics {
    private final String address;
    private volatile long succeeded = 0;
    private volatile long unavailable = 0;
    private volatile long closing = 0;
    private volatile long dead = 0;
    private volatile long npe = 0;
    private volatile long nha = 0;
    private volatile long minMillis = Long.MAX_VALUE;
    private volatile long maxMillis = 0;
    private volatile long totalMillis;

    EndpointConnectionMetrics(String address) {
        this.address = address;
    }

    public void succeeded(long startMillis){
        succeeded++;
        updateTimings(startMillis);
    }

    public void unavailable(long startMillis){
        unavailable++;
        updateTimings(startMillis);
    }

    public void closing(long startMillis){
        closing++;
        updateTimings(startMillis);
    }

    public void dead(long startMillis){
        dead++;
        updateTimings(startMillis);
    }

    public void npe(long startMillis){
        npe++;
        updateTimings(startMillis);
    }

    public void nha(long startMillis){
        nha++;
        updateTimings(startMillis);
    }

    long total(){
        return succeeded + unavailable + closing + dead + npe + nha;
    }

    private void updateTimings(long startMillis){
        long endMillis = System.currentTimeMillis();
        long duration = endMillis - startMillis;
        totalMillis += duration;
        if (duration > maxMillis){
            maxMillis = duration;
        }
        if (duration < minMillis){
            minMillis = duration;
        }
    }

    @Override
    public String toString() {
        long total = total();
        double avg = (double)totalMillis/(double)total;
        return String.format("%s [total: %s, succeeded: %s, unavailable: %s, closing: %s, dead: %s, npe: %s, nha: %s, minMillis: %s, maxMillis: %s, avgMillis: %.2f]",
                address,
                total,
                succeeded,
                unavailable,
                closing,
                dead,
                npe,
                nha,
                minMillis,
                maxMillis,
                avg);
    }
}
