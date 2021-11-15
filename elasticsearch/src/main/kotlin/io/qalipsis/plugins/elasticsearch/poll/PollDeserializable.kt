package io.qalipsis.plugins.elasticsearch.poll

import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.plugins.elasticsearch.ElasticsearchDocument
import kotlin.reflect.KClass

/**
 * Interface of a step that provides a list of items by default but can be amended to deserialize or flatten those lists.
 *
 * @author Eric Jessé
 */
interface PollDeserializable<T> : StepSpecification<Unit, List<ElasticsearchDocument<T>>, PollDeserializable<T>> {

    /**
     * Converts each record of the batch into the provided class and provides the converted batch to the next step.
     *
     * @param targetClass the class of each individual record
     * @param fullDocument when set true, not only the source of the document but the full document is deserialized, including its metadata - defaults to false
     */
    fun <O : Any> deserialize(targetClass: KClass<O>,
                              fullDocument: Boolean = false): StepSpecification<Unit, List<ElasticsearchDocument<O>>, *>

    /**
     * Returns each record of a batch individually to the next steps.
     *
     * @param fullDocument when set true, not only the source of the document but the full document is deserialized, including its metadata - defaults to false
     */
    fun flatten(fullDocument: Boolean = false): StepSpecification<Unit, ElasticsearchDocument<T>, *>

    /**
     * Returns each individual record to the next steps after the conversion.
     *
     * @param targetClass the class of each individual record
     * @param fullDocument when set true, not only the source of the document but the full document is deserialized, including its metadata - defaults to false
     */
    fun <O : Any> flatten(targetClass: KClass<O>,
                          fullDocument: Boolean = false): StepSpecification<Unit, ElasticsearchDocument<O>, *>

}
