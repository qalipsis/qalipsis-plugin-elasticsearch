package io.qalipsis.plugins.elasticsearch

import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.plugins.elasticsearch.query.SearchResult
import kotlin.reflect.KClass

/**
 * Interface of a step that provides a list of documents that can be deserialized to a different type.
 *
 * @author Eric Jessé
 */
interface Deserializable<I, T> : StepSpecification<I, Pair<I, SearchResult<T>>, Deserializable<I, T>> {

    /**
     * Converts each record of the batch into the provided class and provides the converted batch to the next step.
     *
     * @param targetClass the class of each individual record
     * @param fullDocument when set true, not only the source of the document but the full document is deserialized, including its metadata - defaults to false
     */
    fun <O : Any> deserialize(targetClass: KClass<O>,
                              fullDocument: Boolean = false): StepSpecification<I, Pair<I, SearchResult<O>>, *>

}
