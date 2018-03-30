package org.wso2.extension.siddhi.store.redis;

/**
 * Class denoting a stream variable, which will contain a type and a name.
 */
public class StreamVariable {
    private Object name;

    StreamVariable(Object name) {
        this.name = name;
    }

    public String getName() {
        return name.toString();
    }

    public void setName(Object name) {
        this.name = name;
    }
}
