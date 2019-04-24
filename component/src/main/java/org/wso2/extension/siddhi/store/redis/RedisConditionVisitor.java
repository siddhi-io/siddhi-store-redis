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

package org.wso2.extension.siddhi.store.redis;

import io.siddhi.core.exception.OperationNotSupportedException;
import io.siddhi.core.table.record.BaseExpressionVisitor;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.expression.condition.Compare;
import org.wso2.extension.siddhi.store.redis.beans.StoreVariable;
import org.wso2.extension.siddhi.store.redis.beans.StreamVariable;

/**
 * Redis Condition Visitor class
 **/
public class RedisConditionVisitor extends BaseExpressionVisitor {
    private boolean isBeginCompareRightOperand;
    private boolean isStoreVariableOnRight;
    private volatile BasicCompareOperation currentOperation;

    RedisConditionVisitor() {
        currentOperation = new BasicCompareOperation();
    }

    public BasicCompareOperation returnCondition() {
        return currentOperation;
    }

    @Override
    public void beginVisitAnd() {
        throw new OperationNotSupportedException("AND operations are not supported by the Redis Table " +
                "extension. Please check your query and try again");
    }

    @Override
    public void endVisitAnd() {
        //Not applicable
    }

    @Override
    public void beginVisitAndLeftOperand() {
        //Not applicable
    }

    @Override
    public void endVisitAndLeftOperand() {
        //Not applicable
    }

    @Override
    public void beginVisitAndRightOperand() {
        //Not applicable
    }

    @Override
    public void endVisitAndRightOperand() {
        //Not applicable
    }

    @Override
    public void beginVisitOr() {
        throw new OperationNotSupportedException("OR operations are not supported by the Redis Table " +
                "extension. Please check your query and try again");
    }

    @Override
    public void endVisitOr() {
        //Not applicable
    }

    @Override
    public void beginVisitOrLeftOperand() {
        //Not applicable
    }

    @Override
    public void endVisitOrLeftOperand() {
        //Not applicable
    }

    @Override
    public void beginVisitOrRightOperand() {
        //Not applicable
    }

    @Override
    public void endVisitOrRightOperand() {
        //Not applicable
    }

    @Override
    public void beginVisitNot() {
        throw new OperationNotSupportedException("NOT operations are not supported by the Redis Table " +
                "extension. Please check your query and try again");
    }

    @Override
    public void endVisitNot() {
        //Not applicable
    }

    @Override
    public void beginVisitCompare(Compare.Operator operator) {
        this.currentOperation = new BasicCompareOperation();
    }

    @Override
    public void endVisitCompare(Compare.Operator operator) {
        //Not applicable
    }

    @Override
    public void beginVisitCompareLeftOperand(Compare.Operator operator) {
        //Not applicable
    }

    @Override
    public void endVisitCompareLeftOperand(Compare.Operator operator) {
        //Not applicable
    }

    @Override
    public void beginVisitCompareRightOperand(Compare.Operator operator) {
        isBeginCompareRightOperand = true;
    }

    @Override
    public void endVisitCompareRightOperand(Compare.Operator operator) {
        if (!isStoreVariableOnRight) {
            if (operator.equals(Compare.Operator.EQUAL)) {
                currentOperation.setOperator(Compare.Operator.EQUAL);
            } else {
                throw new OperationNotSupportedException("Redis store extension does not support comparison " +
                        "operations other than EQUAL operation");
            }
        } else {
            isStoreVariableOnRight = false;
            if (operator.equals(Compare.Operator.EQUAL)) {
                currentOperation.setOperator(Compare.Operator.EQUAL);
            } else {
                throw new OperationNotSupportedException("Redis store extension does not support comparison " +
                        "operations other than EQUAL operation");
            }
        }
    }

    @Override
    public void beginVisitIsNull(String streamId) {
        throw new OperationNotSupportedException("IsNull operations are not supported by the Redis Table " +
                "extension. Please check your query and try again");
    }

    @Override
    public void endVisitIsNull(String streamId) {
        //Not applicable
    }

    @Override
    public void beginVisitIn(String storeId) {
        throw new OperationNotSupportedException("In operations are not supported by the Redis Table " +
                "extension. Please check your query and try again");
    }

    @Override
    public void endVisitIn(String storeId) {
        //Not applicable
    }

    @Override
    public void beginVisitConstant(Object value, Attribute.Type type) {
        //Not applicable
    }

    @Override
    public void endVisitConstant(Object value, Attribute.Type type) {
        StreamVariable streamVariable = new StreamVariable(value.toString());
        currentOperation.setStreamVariable(streamVariable);
    }

    @Override
    public void beginVisitMath(MathOperator mathOperator) {
        throw new OperationNotSupportedException("Math operations are not supported by the Redis Table " +
                "extension. Please check your query and try again");
    }

    @Override
    public void endVisitMath(MathOperator mathOperator) {
        //Not applicable
    }

    @Override
    public void beginVisitMathLeftOperand(MathOperator mathOperator) {
        //Not applicable
    }

    @Override
    public void endVisitMathLeftOperand(MathOperator mathOperator) {
        //Not applicable
    }

    @Override
    public void beginVisitMathRightOperand(MathOperator mathOperator) {
        //Not applicable
    }

    @Override
    public void endVisitMathRightOperand(MathOperator mathOperator) {
        //Not applicable
    }

    @Override
    public void beginVisitAttributeFunction(String namespace, String functionName) {
        //Not applicable
    }

    @Override
    public void endVisitAttributeFunction(String namespace, String functionName) {
        //Not applicable
    }

    @Override
    public void beginVisitParameterAttributeFunction(int index) {
        //Not applicable
    }

    @Override
    public void endVisitParameterAttributeFunction(int index) {
        //Not applicable
    }

    @Override
    public void beginVisitStreamVariable(String id, String streamId, String attributeName, Attribute.Type type) {
        StreamVariable streamVariable = new StreamVariable(id);
        currentOperation.setStreamVariable(streamVariable);
    }

    @Override
    public void endVisitStreamVariable(String id, String streamId, String attributeName, Attribute.Type type) {
        //Not applicable
    }

    @Override
    public void beginVisitStoreVariable(String storeId, String attributeName, Attribute.Type type) {
        if (isBeginCompareRightOperand) {
            isStoreVariableOnRight = true;
        }
        StoreVariable storeVariable = new StoreVariable(attributeName);
        currentOperation.setStoreVariable(storeVariable);
    }

    @Override
    public void endVisitStoreVariable(String storeId, String attributeName, Attribute.Type type) {
        //Not applicable
    }

}

