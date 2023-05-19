package com.mistra.flink.transformation;

/**
 * @author wrmistra@gmail.com
 * @date 2023/5/19
 * @ Description:
 */
public class AccessLog {

    private int id;

    private String domain;

    private int duration;

    public AccessLog(int id, String domain, int duration) {
        this.id = id;
        this.domain = domain;
        this.duration = duration;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public int getDuration() {
        return duration;
    }

    public void setDuration(int duration) {
        this.duration = duration;
    }

    @Override
    public String toString() {
        return "AccessLog{" +
                "id=" + id +
                ", domain='" + domain + '\'' +
                ", duration=" + duration +
                '}';
    }
}
