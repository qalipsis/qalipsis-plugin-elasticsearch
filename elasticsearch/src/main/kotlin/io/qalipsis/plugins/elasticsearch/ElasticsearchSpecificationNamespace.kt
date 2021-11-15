package io.qalipsis.plugins.elasticsearch

import io.qalipsis.api.scenario.ScenarioSpecification
import io.qalipsis.api.steps.AbstractPluginStepWrapper
import io.qalipsis.api.steps.AbstractScenarioSpecificationWrapper
import io.qalipsis.api.steps.StepSpecification


/**
 * Step wrapper to append to all steps before using a step from the Elasticsearch plugin.
 *
 * @author Eric Jessé
 */
interface ElasticsearchStepSpecification<INPUT, OUTPUT, SELF : StepSpecification<INPUT, OUTPUT, SELF>> :
    StepSpecification<INPUT, OUTPUT, SELF>

/**
 * Step wrapper to append to all steps before using a step from the Elasticsearch plugin.
 *
 * @author Eric Jessé
 */
class ElasticsearchSpecificationImpl<INPUT, OUTPUT>(wrappedStepSpec: StepSpecification<INPUT, OUTPUT, *>) :
    AbstractPluginStepWrapper<INPUT, OUTPUT>(wrappedStepSpec),
    ElasticsearchStepSpecification<INPUT, OUTPUT, AbstractPluginStepWrapper<INPUT, OUTPUT>>

fun <INPUT, OUTPUT> StepSpecification<INPUT, OUTPUT, *>.elasticsearch(): ElasticsearchStepSpecification<INPUT, OUTPUT, *> =
    ElasticsearchSpecificationImpl(this)

/**
 * Scenario wrapper to append to a scenario before using a step from the Elasticsearch plugin.
 *
 * @author Eric Jessé
 */
class ElasticsearchScenarioSpecification(scenario: ScenarioSpecification) :
    AbstractScenarioSpecificationWrapper(scenario)

fun ScenarioSpecification.elasticsearch() = ElasticsearchScenarioSpecification(this)
