/*
 * Copyright 2022 AERIS IT Solutions GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

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
