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
