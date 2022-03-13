package com.aliyun.iecs.model;

import java.io.Serializable;

public class FenceData implements Serializable {
    public String geohash;
    public String node_id;
    public String block_type;
    public String block_code;
    public String block_bay;
    public String block_pos;
    public String fence;

    public FenceData newdata(String geohash) {
        FenceData fenceData = new FenceData();
        fenceData.setGeohash(geohash);
        fenceData.setNode_id(this.getNode_id());
        fenceData.setBlock_type(this.getBlock_type());
        fenceData.setBlock_code(this.getBlock_code());
        fenceData.setBlock_bay(this.getBlock_bay());
        fenceData.setBlock_pos(this.getBlock_pos());
        fenceData.setFence(this.getFence());
        return fenceData;
    }

    public String getGeohash() {
        return this.geohash;
    }

    public void setGeohash(String geohash) {
        this.geohash = geohash;
    }

    public String getNode_id() {
        return this.node_id;
    }

    public void setNode_id(String node_id) {
        this.node_id = node_id;
    }

    public String getBlock_type() {
        return this.block_type;
    }

    public void setBlock_type(String block_type) {
        this.block_type = block_type;
    }

    public String getBlock_code() {
        return this.block_code;
    }

    public void setBlock_code(String block_code) {
        this.block_code = block_code;
    }

    public String getBlock_bay() {
        return this.block_bay;
    }

    public void setBlock_bay(String block_bay) {
        this.block_bay = block_bay;
    }

    public String getBlock_pos() {
        return this.block_pos;
    }

    public void setBlock_pos(String block_pos) {
        this.block_pos = block_pos;
    }

    public String getFence() {
        return this.fence;
    }

    public void setFence(String fence) {
        this.fence = fence;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            FenceData fenceData;
            label89: {
                fenceData = (FenceData)o;
                if (this.geohash != null) {
                    if (this.geohash.equals(fenceData.geohash)) {
                        break label89;
                    }
                } else if (fenceData.geohash == null) {
                    break label89;
                }

                return false;
            }

            label82: {
                if (this.node_id != null) {
                    if (this.node_id.equals(fenceData.node_id)) {
                        break label82;
                    }
                } else if (fenceData.node_id == null) {
                    break label82;
                }

                return false;
            }

            if (this.block_type != null) {
                if (!this.block_type.equals(fenceData.block_type)) {
                    return false;
                }
            } else if (fenceData.block_type != null) {
                return false;
            }

            if (this.block_code != null) {
                if (!this.block_code.equals(fenceData.block_code)) {
                    return false;
                }
            } else if (fenceData.block_code != null) {
                return false;
            }

            label61: {
                if (this.block_bay != null) {
                    if (this.block_bay.equals(fenceData.block_bay)) {
                        break label61;
                    }
                } else if (fenceData.block_bay == null) {
                    break label61;
                }

                return false;
            }

            if (this.block_pos != null) {
                if (!this.block_pos.equals(fenceData.block_pos)) {
                    return false;
                }
            } else if (fenceData.block_pos != null) {
                return false;
            }

            return this.fence != null ? this.fence.equals(fenceData.fence) : fenceData.fence == null;
        } else {
            return false;
        }
    }

    public int hashCode() {
        int result = this.geohash != null ? this.geohash.hashCode() : 0;
        result = 31 * result + (this.node_id != null ? this.node_id.hashCode() : 0);
        result = 31 * result + (this.block_type != null ? this.block_type.hashCode() : 0);
        result = 31 * result + (this.block_code != null ? this.block_code.hashCode() : 0);
        result = 31 * result + (this.block_bay != null ? this.block_bay.hashCode() : 0);
        result = 31 * result + (this.block_pos != null ? this.block_pos.hashCode() : 0);
        result = 31 * result + (this.fence != null ? this.fence.hashCode() : 0);
        return result;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer("FenceData{");
        sb.append("geohash='").append(this.geohash).append('\'');
        sb.append(", node_id='").append(this.node_id).append('\'');
        sb.append(", block_type='").append(this.block_type).append('\'');
        sb.append(", block_code='").append(this.block_code).append('\'');
        sb.append(", block_bay='").append(this.block_bay).append('\'');
        sb.append(", block_pos='").append(this.block_pos).append('\'');
        sb.append(", fence='").append(this.fence).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public FenceData() {
    }
}