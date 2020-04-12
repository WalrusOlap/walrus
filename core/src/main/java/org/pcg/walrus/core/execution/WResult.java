package org.pcg.walrus.core.execution;

/**
 * walrus job result
 */
public class WResult {

    private long lines;
    private String saveMode;
    private String savePath;

    public String getSaveMode() {
        return saveMode;
    }

    public void setSaveMode(String saveMode) {
        this.saveMode = saveMode;
    }

    public String getSavePath() {
        return savePath;
    }

    public void setSavePath(String savePath) {
        this.savePath = savePath;
    }

    public long getLines() {
        return lines;
    }

    public void setLines(long lines) {
        this.lines = lines;
    }

}
