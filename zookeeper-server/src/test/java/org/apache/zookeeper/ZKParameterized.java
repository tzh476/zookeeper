package org.apache.zookeeper;

import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;
import org.junit.runners.parameterized.BlockJUnit4ClassRunnerWithParameters;
import org.junit.runners.parameterized.BlockJUnit4ClassRunnerWithParametersFactory;
import org.junit.runners.parameterized.TestWithParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ZKParameterized {
    private static final Logger LOG = LoggerFactory.getLogger(ZKParameterized.class);
    public static class RunnerFactory extends BlockJUnit4ClassRunnerWithParametersFactory {
        @Override
        public org.junit.runner.Runner createRunnerForTestWithParameters(TestWithParameters test) throws InitializationError {
            return new ZKParameterized.Runner(test);
        }
    }

    public static class Runner extends BlockJUnit4ClassRunnerWithParameters {
        public Runner(TestWithParameters test) throws InitializationError {
            super(test);
        }


        @Override
        protected List<FrameworkMethod> computeTestMethods() {
            return JUnit4ZKTestRunner.computeTestMethodsForClass(getTestClass().getJavaClass(), super.computeTestMethods());
        }


        @Override
        protected Statement methodInvoker(FrameworkMethod method, Object test) {
            return new JUnit4ZKTestRunner.LoggedInvokeMethod(method, test);
        }
    }
}
