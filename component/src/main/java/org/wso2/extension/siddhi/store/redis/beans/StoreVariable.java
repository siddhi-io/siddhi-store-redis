/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
