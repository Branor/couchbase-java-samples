package samples.crud;

import java.util.UUID;

/**
 * Created by David on 22/09/2014.
 */
public class Data {
    private UUID id;
    private String text;
    private double number;
    private String type;

    public Data(UUID id, String text, double number, String type) {
        this.id = id;
        this.text = text;
        this.number = number;
        this.type = type;
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public double getNumber() {
        return number;
    }

    public void setNumber(double number) {
        this.number = number;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
