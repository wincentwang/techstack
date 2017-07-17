package wang.wincent.techstack.storm.example.orderMonitor.domain;

/**
 * Describe: 请补充类描述
 */
public class Trigger {
    private String orderId;
    private String ruleId;

    public Trigger() {
    }

    public Trigger(String orderId, String ruleId) {
        this.orderId = orderId;
        this.ruleId = ruleId;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getRuleId() {
        return ruleId;
    }

    public void setRuleId(String ruleId) {
        this.ruleId = ruleId;
    }

    @Override
    public String toString() {
        return "Trigger{" +
                "orderId='" + orderId + '\'' +
                ", ruleId='" + ruleId + '\'' +
                '}';
    }
}
