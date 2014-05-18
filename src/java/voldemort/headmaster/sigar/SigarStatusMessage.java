package voldemort.headmaster.sigar;

import java.io.Serializable;

public class SigarStatusMessage implements Serializable{

    private Double CPU;
    private Double HDD;
    private Double RAM;
    private String hostname;

    public SigarStatusMessage(double CPU, double HDD, double RAM, String hostname){
        this.CPU = CPU;
        this.HDD = HDD;
        this.RAM = RAM;
        this.hostname = hostname;
    }

    public Double getCPU() {
        return CPU;
    }

    public double getHDD() {
        return HDD;
    }

    public double getRAM() {
        return RAM;
    }

    public String getHostname() {
        return hostname;
    }

    @Override
    public String toString() {
        return String.format("%s: CPU: %.2f RAM: %.2f HDD: %.2f", hostname, CPU, RAM, HDD);
    }



}
