package org.wso2.extension.siddhi.store.redis.beans;


/**
 * Class denoting a store variable and constant, which will have a type and a name. This is kept separate from a
 * stream variable
 * even though they contain the same fields because there are cases where we need to distinguish between the two.
 */
public class StoreVariable {
    private String name;

    public StoreVariable(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

}
