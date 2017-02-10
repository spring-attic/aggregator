/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.app.aggregator;

import java.util.Collections;

import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.embedded.EmbeddedMongoAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.CustomConversions;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.integration.gemfire.store.GemfireMessageStore;
import org.springframework.integration.jdbc.JdbcMessageStore;
import org.springframework.integration.mongodb.store.ConfigurableMongoDbMessageStore;
import org.springframework.integration.mongodb.support.MongoDbMessageBytesConverter;
import org.springframework.integration.redis.store.RedisMessageStore;
import org.springframework.integration.store.MessageGroupStore;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.StringUtils;

import com.gemstone.gemfire.cache.Region;

/**
 * A helper class containing configuration classes for particular technologies
 * to expose an appropriate {@link org.springframework.integration.store.MessageStore} bean
 * via matched configuration properties.
 *
 * @author Artem Bilan
 */
class MessageStoreConfiguration {

	@ConditionalOnClass(ConfigurableMongoDbMessageStore.class)
	@ConditionalOnProperty(prefix = AggregatorProperties.PREFIX,
			name = "messageStoreType",
			havingValue = AggregatorProperties.MessageStoreType.MONGODB)
	@Import({
			MongoAutoConfiguration.class,
			MongoDataAutoConfiguration.class,
			EmbeddedMongoAutoConfiguration.class })
	static class Mongo {

		@Bean
		public MessageGroupStore messageStore(MongoTemplate mongoTemplate, AggregatorProperties properties) {
			if (StringUtils.hasText(properties.getMessageStoreEntity())) {
				return new ConfigurableMongoDbMessageStore(mongoTemplate, properties.getMessageStoreEntity());
			}
			else {
				return new ConfigurableMongoDbMessageStore(mongoTemplate);
			}
		}

		@Bean
		public CustomConversions customConversions() {
			return new CustomConversions(Collections.singletonList(new MongoDbMessageBytesConverter()));
		}

	}

	@ConditionalOnClass(RedisMessageStore.class)
	@ConditionalOnProperty(prefix = AggregatorProperties.PREFIX,
			name = "messageStoreType",
			havingValue = AggregatorProperties.MessageStoreType.REDIS)
	@Import(RedisAutoConfiguration.class)
	static class Redis {

		@Bean
		public MessageGroupStore messageStore(RedisTemplate<?, ?> redisTemplate) {
			return new RedisMessageStore(redisTemplate.getConnectionFactory());
		}

	}

	@ConditionalOnClass(GemfireMessageStore.class)
	@ConditionalOnProperty(prefix = AggregatorProperties.PREFIX,
			name = "messageStoreType",
			havingValue = AggregatorProperties.MessageStoreType.GEMFIRE)
	static class Gemfire {

		@Bean
		public MessageGroupStore messageStore(Region<Object, Object> region) {
			return new GemfireMessageStore(region);
		}

	}

	@ConditionalOnClass(JdbcMessageStore.class)
	@ConditionalOnProperty(prefix = AggregatorProperties.PREFIX,
			name = "messageStoreType",
			havingValue = AggregatorProperties.MessageStoreType.JDBC)
	@Import({
			DataSourceAutoConfiguration.class,
			DataSourceTransactionManagerAutoConfiguration.class })
	static class Jdbc {

		@Bean
		public MessageGroupStore messageStore(JdbcTemplate jdbcTemplate, AggregatorProperties properties) {
			JdbcMessageStore messageStore = new JdbcMessageStore();
			messageStore.setJdbcTemplate(jdbcTemplate);
			if (StringUtils.hasText(properties.getMessageStoreEntity())) {
				messageStore.setTablePrefix(properties.getMessageStoreEntity());
			}
			return messageStore;
		}

	}

}
