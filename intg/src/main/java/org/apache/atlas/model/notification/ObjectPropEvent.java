package org.apache.atlas.model.notification;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import java.io.Serializable;
import java.util.HashMap;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class ObjectPropEvent implements Serializable {

    PropagationOperationType operation;
    HashMap<String, Object> payload;

    @Override
    public String toString() {
        return "ObjectPropEvent{" +
                "operation=" + operation +
                ", payload=" + payload +
                '}';
    }

    public ObjectPropEvent(PropagationOperationType operation, HashMap<String, Object> payload) {
        this.operation = operation;
        this.payload = payload;
    }

    public ObjectPropEvent() {
    }

    public PropagationOperationType getOperation() {
        return operation;
    }

    public void setOperation(PropagationOperationType operation) {
        this.operation = operation;
    }

    public HashMap<String, Object> getPayload() {
        return payload;
    }

    public void setPayload(HashMap<String, Object> payload) {
        this.payload = payload;
    }

    public void normalize() {
    }

    public enum PropagationOperationType {

        CLASSIFICATION_PROPAGATION_TEXT_UPDATE("CLASSIFICATION_PROPAGATION_TEXT_UPDATE"),
        CLASSIFICATION_PROPAGATION_ADD("CLASSIFICATION_PROPAGATION_ADD"),
        CLASSIFICATION_PROPAGATION_DELETE("CLASSIFICATION_PROPAGATION_DELETE"),
        CLASSIFICATION_ONLY_PROPAGATION_DELETE("CLASSIFICATION_ONLY_PROPAGATION_DELETE"),
        CLASSIFICATION_ONLY_PROPAGATION_DELETE_ON_HARD_DELETE("CLASSIFICATION_ONLY_PROPAGATION_DELETE_ON_HARD_DELETE"),
        CLASSIFICATION_REFRESH_PROPAGATION("CLASSIFICATION_REFRESH_PROPAGATION"),
        CLASSIFICATION_PROPAGATION_RELATIONSHIP_UPDATE("CLASSIFICATION_PROPAGATION_RELATIONSHIP_UPDATE"),
        CLEANUP_CLASSIFICATION_PROPAGATION("CLEANUP_CLASSIFICATION_PROPAGATION");

        private final String value;

        PropagationOperationType(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return value;
        }
    }
}
