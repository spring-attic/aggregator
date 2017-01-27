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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.context.annotation.Bean;
import org.springframework.data.gemfire.CacheFactoryBean;
import org.springframework.data.gemfire.LocalRegionFactoryBean;
import org.springframework.data.gemfire.RegionFactoryBean;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.aggregator.AggregatingMessageHandler;
import org.springframework.integration.gemfire.store.GemfireMessageStore;
import org.springframework.integration.jdbc.JdbcMessageStore;
import org.springframework.integration.mongodb.store.ConfigurableMongoDbMessageStore;
import org.springframework.integration.redis.store.RedisMessageStore;
import org.springframework.integration.store.MessageGroupStore;
import org.springframework.integration.store.SimpleMessageStore;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import com.gemstone.gemfire.cache.Cache;

/**
 * Tests for the Aggregator Processor.
 *
 * @author Artem Bilan
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@DirtiesContext
public abstract class AggregatorTests {

	@Autowired
	protected Processor channels;

	@Autowired
	protected MessageCollector collector;

	@Autowired(required = false)
	protected MessageGroupStore messageGroupStore;

	@Autowired
	protected AggregatingMessageHandler aggregatingMessageHandler;

	public static class DefaultAggregatorTests extends AggregatorTests {

		@Test
		public void test() throws Exception {
			this.channels.input()
					.send(MessageBuilder.withPayload("2")
							.setHeader(IntegrationMessageHeaderAccessor.CORRELATION_ID, "my_correlation")
							.setHeader(IntegrationMessageHeaderAccessor.SEQUENCE_NUMBER, 2)
							.setHeader(IntegrationMessageHeaderAccessor.SEQUENCE_SIZE, 2)
							.build());

			this.channels.input()
					.send(MessageBuilder.withPayload("1")
							.setHeader(IntegrationMessageHeaderAccessor.CORRELATION_ID, "my_correlation")
							.setHeader(IntegrationMessageHeaderAccessor.SEQUENCE_NUMBER, 1)
							.setHeader(IntegrationMessageHeaderAccessor.SEQUENCE_SIZE, 2)
							.build());


			Message<?> out = this.collector.forChannel(this.channels.output())
					.poll(10, TimeUnit.SECONDS);

			assertThat(out, notNullValue());
			assertThat(out.getPayload(), instanceOf(List.class));
			@SuppressWarnings("unchecked")
			List<String> result = (List<String>) out.getPayload();
			Collections.sort(result);
			assertThat(result.size(), equalTo(2));
			assertThat(result, contains("1", "2"));

			assertNull(this.messageGroupStore);

			assertThat(this.aggregatingMessageHandler.getMessageStore(), instanceOf(SimpleMessageStore.class));
		}

	}

	@TestPropertySource(properties = {
			"spring.data.mongodb.port=0",
			"aggregator.correlation=T(Thread).currentThread().id",
			"aggregator.release=!#this.?[payload == 'bar'].empty",
			"aggregator.aggregation=#this.?[payload == 'foo'].![payload]",
			"aggregator.message-store-type=mongodb",
			"aggregator.message-store-entity=aggregatorTest" })
	public static class CustomPropsAndMongoMessageStoreAggregatorTests extends AggregatorTests {

		@Test
		public void test() throws Exception {
			this.channels.input()
					.send(new GenericMessage<>("foo"));

			this.channels.input()
					.send(new GenericMessage<>("bar"));


			Message<?> out = this.collector.forChannel(this.channels.output())
					.poll(10, TimeUnit.SECONDS);

			assertThat(out, notNullValue());
			assertThat(out.getPayload(), instanceOf(List.class));
			@SuppressWarnings("unchecked")
			List<String> result = (List<String>) out.getPayload();
			Collections.sort(result);
			assertThat(result.size(), equalTo(1));
			assertThat(result.get(0), equalTo("foo"));

			assertThat(this.messageGroupStore, instanceOf(ConfigurableMongoDbMessageStore.class));
			assertThat(TestUtils.getPropertyValue(this.messageGroupStore, "collectionName", String.class),
					equalTo("aggregatorTest"));

			assertThat(this.aggregatingMessageHandler.getMessageStore(), sameInstance(this.messageGroupStore));
		}

	}

	@TestPropertySource(properties = {
			"aggregator.message-store-type=gemfire",
			"aggregator.groupTimeout=10" })
	public static class GroupTimeOutAndGemfireMessageStoreAggregatorTests extends AggregatorTests {

		@Test
		public void test() throws Exception {
			this.channels.input()
					.send(MessageBuilder.withPayload("1")
							.setHeader(IntegrationMessageHeaderAccessor.CORRELATION_ID, "my_correlation")
							.build());


			Message<?> out = this.collector.forChannel(this.channels.output())
					.poll(10, TimeUnit.SECONDS);

			assertThat(out, notNullValue());
			assertThat(out.getPayload(), instanceOf(List.class));
			@SuppressWarnings("unchecked")
			List<String> result = (List<String>) out.getPayload();
			Collections.sort(result);
			assertThat(result.size(), equalTo(1));
			assertThat(result, contains("1"));

			assertThat(this.messageGroupStore, instanceOf(GemfireMessageStore.class));
			assertThat(this.aggregatingMessageHandler.getMessageStore(), sameInstance(this.messageGroupStore));
		}

	}

	@TestPropertySource(properties = "aggregator.message-store-type=redis")
	@Ignore("Needs real Redis Server to be run")
	public static class RedisMessageStoreAggregatorTests extends AggregatorTests {

		@Test
		public void test() throws Exception {
			this.channels.input()
					.send(MessageBuilder.withPayload("2")
							.setHeader(IntegrationMessageHeaderAccessor.CORRELATION_ID, "my_correlation")
							.setHeader(IntegrationMessageHeaderAccessor.SEQUENCE_NUMBER, 2)
							.setHeader(IntegrationMessageHeaderAccessor.SEQUENCE_SIZE, 2)
							.build());

			this.channels.input()
					.send(MessageBuilder.withPayload("1")
							.setHeader(IntegrationMessageHeaderAccessor.CORRELATION_ID, "my_correlation")
							.setHeader(IntegrationMessageHeaderAccessor.SEQUENCE_NUMBER, 1)
							.setHeader(IntegrationMessageHeaderAccessor.SEQUENCE_SIZE, 2)
							.build());


			Message<?> out = this.collector.forChannel(this.channels.output())
					.poll(10, TimeUnit.SECONDS);

			assertThat(out, notNullValue());
			assertThat(out.getPayload(), instanceOf(List.class));
			@SuppressWarnings("unchecked")
			List<String> result = (List<String>) out.getPayload();
			Collections.sort(result);
			assertThat(result.size(), equalTo(2));
			assertThat(result, contains("1", "2"));

			assertThat(this.messageGroupStore, instanceOf(RedisMessageStore.class));

			assertThat(this.aggregatingMessageHandler.getMessageStore(), sameInstance(this.messageGroupStore));
		}

	}

	@TestPropertySource(properties = {
			"aggregator.message-store-type=jdbc",
			"spring.datasource.url=jdbc:h2:mem:test",
			"spring.datasource.schema=org/springframework/integration/jdbc/schema-h2.sql"})
	public static class JdbcMessageStoreAggregatorTests extends AggregatorTests {

		@Test
		public void test() throws Exception {
			this.channels.input()
					.send(MessageBuilder.withPayload("2")
							.setHeader(IntegrationMessageHeaderAccessor.CORRELATION_ID, "my_correlation")
							.setHeader(IntegrationMessageHeaderAccessor.SEQUENCE_NUMBER, 2)
							.setHeader(IntegrationMessageHeaderAccessor.SEQUENCE_SIZE, 2)
							.build());

			this.channels.input()
					.send(MessageBuilder.withPayload("1")
							.setHeader(IntegrationMessageHeaderAccessor.CORRELATION_ID, "my_correlation")
							.setHeader(IntegrationMessageHeaderAccessor.SEQUENCE_NUMBER, 1)
							.setHeader(IntegrationMessageHeaderAccessor.SEQUENCE_SIZE, 2)
							.build());


			Message<?> out = this.collector.forChannel(this.channels.output())
					.poll(10, TimeUnit.SECONDS);

			assertThat(out, notNullValue());
			assertThat(out.getPayload(), instanceOf(List.class));
			@SuppressWarnings("unchecked")
			List<String> result = (List<String>) out.getPayload();
			Collections.sort(result);
			assertThat(result.size(), equalTo(2));
			assertThat(result, contains("1", "2"));

			assertThat(this.messageGroupStore, instanceOf(JdbcMessageStore.class));

			assertThat(this.aggregatingMessageHandler.getMessageStore(), sameInstance(this.messageGroupStore));
		}

	}

	@SpringBootApplication
	public static class DefaultAggregatorApplication {

		@Bean
		@ConditionalOnProperty(prefix = AggregatorProperties.PREFIX,
				name = "messageStoreType",
				havingValue = AggregatorProperties.MessageStoreType.GEMFIRE)
		public CacheFactoryBean gemfireCache() {
			return new CacheFactoryBean();
		}

		@Bean
		@ConditionalOnProperty(prefix = AggregatorProperties.PREFIX,
				name = "messageStoreType",
				havingValue = AggregatorProperties.MessageStoreType.GEMFIRE)
		public RegionFactoryBean<?, ?> gemfireRegion(Cache cache) {
			LocalRegionFactoryBean<?, ?> localRegionFactoryBean = new LocalRegionFactoryBean<>();
			localRegionFactoryBean.setCache(cache);
			localRegionFactoryBean.setName("aggregatorTest");
			return localRegionFactoryBean;
		}

	}

}
