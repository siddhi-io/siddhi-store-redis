package org.wso2.extension.siddhi.store.redis;

import org.wso2.extension.siddhi.store.redis.utils.RedisTableConstants;
import org.wso2.siddhi.core.table.record.BaseExpressionVisitor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.expression.condition.Compare;

import java.util.ArrayList;
import java.util.List;

/**
 * Redis Condition Visitor class
 **/
public class RedisConditionVisitor extends BaseExpressionVisitor {

    private List<String> conditionsList;
    private boolean isBeginCompareRightOperand;
    private boolean isStoreVariableOnRight;
    private String currentStoreVariable;
    private String currentStreamVariable;

    public RedisConditionVisitor() {
        conditionsList = new ArrayList<>();
    }

    public List<String> returnCondition() {
        return conditionsList;
    }

    @Override
    public void beginVisitAnd() {
    }

    @Override
    public void endVisitAnd() {
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
        conditionsList.add(RedisTableConstants.JAVA_AND);
    }

    @Override
    public void endVisitAndRightOperand() {
        //Not applicable
    }

    @Override
    public void beginVisitOr() {

    }

    @Override
    public void endVisitOr() {

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
        conditionsList.add(RedisTableConstants.JAVA_OR);
    }

    @Override
    public void endVisitOrRightOperand() {
        //Not applicable
    }

    @Override
    public void beginVisitNot() {
        conditionsList.add(RedisTableConstants.JAVA_NOT);
    }

    @Override
    public void endVisitNot() {
        //Not applicable
    }

    @Override
    public void beginVisitCompare(Compare.Operator operator) {

    }

    @Override
    public void endVisitCompare(Compare.Operator operator) {

//        condition.append(RedisTableConstants.CLOSE_PARENTHESIS);
    }

    @Override
    public void beginVisitCompareLeftOperand(Compare.Operator operator) {
        if (operator == Compare.Operator.NOT_EQUAL) {
            conditionsList.add(RedisTableConstants.NOT_EQUAL);

        }
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
            conditionsList.add(currentStoreVariable);
            switch (operator) {
                case EQUAL:
                    conditionsList.add(RedisTableConstants.EQUAL);
                    break;
                case GREATER_THAN:
                    conditionsList.add(RedisTableConstants.GREATER_THAN);
                    break;
                case GREATER_THAN_EQUAL:
                    conditionsList.add(RedisTableConstants.GREATER_THAN_EQUAL);
                    break;
                case LESS_THAN:
                    conditionsList.add(RedisTableConstants.LESS_THAN);
                    break;
                case LESS_THAN_EQUAL:
                    conditionsList.add(RedisTableConstants.LESS_THAN_EQUAL);
                    break;
                case NOT_EQUAL:
                    conditionsList.add(RedisTableConstants.NOT_EQUAL);
                    break;
            }
            conditionsList.add(currentStreamVariable);
        } else {
            isStoreVariableOnRight = false;
            conditionsList.add(currentStoreVariable);
            switch (operator) {
                case EQUAL:
                    conditionsList.add(RedisTableConstants.EQUAL);
                    break;
                case GREATER_THAN:
                    conditionsList.add(RedisTableConstants.LESS_THAN);
                    break;
                case GREATER_THAN_EQUAL:
                    conditionsList.add(RedisTableConstants.LESS_THAN_EQUAL);
                    break;
                case LESS_THAN:
                    conditionsList.add(RedisTableConstants.GREATER_THAN);
                    break;
                case LESS_THAN_EQUAL:
                    conditionsList.add(RedisTableConstants.GREATER_THAN_EQUAL);
                    break;
                case NOT_EQUAL:
                    conditionsList.add(RedisTableConstants.NOT_EQUAL);
                    break;
            }
            conditionsList.add(currentStreamVariable);
        }
    }

    @Override
    public void beginVisitIsNull(String streamId) {
        conditionsList.add(RedisTableConstants.JAVA_NULL);
    }

    @Override
    public void endVisitIsNull(String streamId) {
        conditionsList.add(currentStoreVariable);
        conditionsList.add(RedisTableConstants.JAVA_NULL);
    }

    @Override
    public void beginVisitIn(String storeId) {
//        condition.append(RDBMSTableConstants.SQL_IN).append(RDBMSTableConstants.WHITESPACE);
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
        currentStreamVariable = value.toString();
    }

    @Override
    public void beginVisitMath(MathOperator mathOperator) {

        //Not applicable
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
        currentStreamVariable = RedisTableConstants.OPEN_SQURE_BRACKETS + id + RedisTableConstants.CLOSE_SQURE_BRACKETS;
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
        currentStoreVariable = attributeName;
    }

    @Override
    public void endVisitStoreVariable(String storeId, String attributeName, Attribute.Type type) {
        //Not applicable
    }

}

