package voldemort.headmaster.sigar;

import org.apache.log4j.Logger;
import org.hyperic.sigar.*;

import java.text.DecimalFormat;

public class LocalNodeStatus {
    private static final Logger logger = Logger.getLogger(LocalNodeStatus.class);

    private static Sigar sigar;
    private static Long BYTES_TO_MB = 1024*1024L;
    private static double max_disk_space_used = 2000;

    public static final String VOLDEMORT_DATA_DIR = "/home/king/src/voldemort/data/bdb";
    public static final String DISK_USAGE = "/";

    private String monitorDir;

    public LocalNodeStatus(String monitorDir) {
        this();
        this.monitorDir = monitorDir;
    }

    public LocalNodeStatus() {
        sigar = new Sigar();
        monitorDir = VOLDEMORT_DATA_DIR;
    }

    public Double getMemoryUsage() {
        Double memUsed = 0.0;
        Mem mem;
        try {
            mem = sigar.getMem();

            memUsed = LocalNodeStatus.doubleFormatted(mem.getUsedPercent());
        } catch (Exception e) {
            logger.error("Failed to get memory usage ", e);
        }
        return memUsed;
    }

    public Double getCPUUsage() {
        Double cpuUsed = 0.0;

        try {
            cpuUsed = LocalNodeStatus.doubleFormatted(sigar.getCpuPerc().getCombined());
        } catch (SigarException e) {
            logger.error("Failed to retrieve CPU-usage ", e);
        }
        return cpuUsed;
    }

    public Double getDiskUsage() {
        Long spaceInBytes = 0L;

        try {
            DirUsage dirUsage;
            dirUsage = sigar.getDirUsage(monitorDir);
            spaceInBytes = dirUsage.getDiskUsage();

//            DiskUsage diskUsage = sigar.getDiskUsage(DISK_USAGE);

        } catch (SigarException e) {
            logger.error("Failed to retrieve disk space used in megabytes ", e);
        }

        Double diskUsed = ((spaceInBytes / BYTES_TO_MB.doubleValue()) / max_disk_space_used) * 100;
        diskUsed = LocalNodeStatus.doubleFormatted(diskUsed);
        return diskUsed;
    }

    private static Double doubleFormatted(Double val) {
        DecimalFormat twoDForm;
        try {
            twoDForm = new DecimalFormat("#.###");
            return Double.valueOf(twoDForm.format(val));
        } catch (Exception e) {
        }

        try {
            twoDForm = new DecimalFormat("#,###");
            return Double.valueOf(twoDForm.format(val));

        } catch (Exception ex) {
            logger.error("failed converting decimal", ex);
        }
        return 0.0;
    }
}


