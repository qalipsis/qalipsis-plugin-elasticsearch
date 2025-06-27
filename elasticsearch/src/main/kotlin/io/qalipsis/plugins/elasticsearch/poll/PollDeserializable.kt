/*
 * QALIPSIS
 * Copyright (C) 2025 AERIS IT Solutions GmbH
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package io.qalipsis.plugins.elasticsearch.poll

import io.qalipsis.api.steps.ConfigurableStepSpecification
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.plugins.elasticsearch.ElasticsearchDocument
import kotlin.reflect.KClass

/**
 * Interface of a step that provides a list of items by default but can be amended to deserialize or flatten those lists.
 *
 * @author Eric Jessé
 */
interface PollDeserializable<T> : StepSpecification<Unit, List<ElasticsearchDocument<T>>, PollDeserializable<T>>,
    ConfigurableStepSpecification<Unit, List<ElasticsearchDocument<T>>, PollDeserializable<T>> {

    /**
     * Converts each record of the batch into the provided class and provides the converted batch to the next step.
     *
     * @param targetClass the class of each individual record
     * @param fullDocument when set true, not only the source of the document but the full document is deserialized, including its metadata - defaults to false
     */
    fun <O : Any> deserialize(
        targetClass: KClass<O>,
        fullDocument: Boolean = false
    ): StepSpecification<Unit, List<ElasticsearchDocument<O>>, *>

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
    fun <O : Any> flatten(
        targetClass: KClass<O>,
        fullDocument: Boolean = false
    ): StepSpecification<Unit, ElasticsearchDocument<O>, *>

}
