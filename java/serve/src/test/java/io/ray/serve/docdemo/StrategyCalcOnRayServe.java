package io.ray.serve.docdemo;

import io.ray.serve.api.Serve;
import io.ray.serve.deployment.Application;
import io.ray.serve.deployment.Deployment;
import io.ray.serve.handle.DeploymentResponse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class StrategyCalcOnRayServe {

  // docs-deploy-start
  public void deploy() {
    Serve.start(null);

    Application deployment =
        Serve.deployment()
            .setName("strategy")
            .setDeploymentDef(StrategyOnRayServe.class.getName())
            .setNumReplicas(4)
            .bind();
    Serve.run(deployment);
  }
  // docs-deploy-end

  // docs-calc-start
  public List<String> calc(Long time, Map<String, List<List<String>>> banksAndIndicators) {
    Deployment deployment = Serve.getDeployment("strategy");

    List<String> results = new ArrayList<>();
    for (Entry<String, List<List<String>>> e : banksAndIndicators.entrySet()) {
      String bank = e.getKey();
      for (List<String> indicators : e.getValue()) {
        for (String indicator : indicators) {
          results.add(
              (String)
                  deployment
                      .getHandle()
                      .method("calcIndicator")
                      .remote(time, bank, indicator)
                      .result());
        }
      }
    }
    return results;
  }
  // docs-calc-end

  // docs-parallel-calc-start
  public List<String> parallelCalc(Long time, Map<String, List<List<String>>> banksAndIndicators) {
    Deployment deployment = Serve.getDeployment("strategy");

    List<String> results = new ArrayList<>();
    List<DeploymentResponse> responses = new ArrayList<>();
    for (Entry<String, List<List<String>>> e : banksAndIndicators.entrySet()) {
      String bank = e.getKey();
      for (List<String> indicators : e.getValue()) {
        for (String indicator : indicators) {
          responses.add(
              deployment.getHandle().method("calcIndicator").remote(time, bank, indicator));
        }
      }
    }
    for (DeploymentResponse response : responses) {
      results.add((String) response.result());
    }
    return results;
  }
  // docs-parallel-calc-end

  // docs-main-start
  public static void main(String[] args) {

    long time = System.currentTimeMillis();
    String bank1 = "demo_bank_1";
    String bank2 = "demo_bank_2";
    String indicator1 = "demo_indicator_1";
    String indicator2 = "demo_indicator_2";
    Map<String, List<List<String>>> banksAndIndicators = new HashMap<>();
    banksAndIndicators.put(bank1, Arrays.asList(Arrays.asList(indicator1, indicator2)));
    banksAndIndicators.put(
        bank2, Arrays.asList(Arrays.asList(indicator1), Arrays.asList(indicator2)));

    StrategyCalcOnRayServe strategy = new StrategyCalcOnRayServe();
    strategy.deploy();
    List<String> results = strategy.parallelCalc(time, banksAndIndicators);

    System.out.println(results);
  }
  // docs-main-end
}
