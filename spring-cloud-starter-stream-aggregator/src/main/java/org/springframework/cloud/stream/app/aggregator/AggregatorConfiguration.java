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

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.integration.aggregator.CorrelationStrategy;
import org.springframework.integration.aggregator.DefaultAggregatingMessageGroupProcessor;
import org.springframework.integration.aggregator.ExpressionEvaluatingCorrelationStrategy;
import org.springframework.integration.aggregator.ExpressionEvaluatingMessageGroupProcessor;
import org.springframework.integration.aggregator.ExpressionEvaluatingReleaseStrategy;
import org.springframework.integration.aggregator.MessageGroupProcessor;
import org.springframework.integration.aggregator.ReleaseStrategy;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.config.AggregatorFactoryBean;
import org.springframework.integration.store.MessageGroupStore;

/**
 * A Processor app that performs aggregation.
 *
 * @author Artem Bilan
 */
@EnableBinding(Processor.class)
@EnableConfigurationProperties(AggregatorProperties.class)
public class AggregatorConfiguration {

	@Autowired
	private AggregatorProperties properties;

	@Autowired
	private BeanFactory beanFactory;

	@Bean
	@ServiceActivator(inputChannel = Processor.INPUT)
	public AggregatorFactoryBean aggregator(
			ObjectProvider<CorrelationStrategy> correlationStrategy,
			ObjectProvider<ReleaseStrategy> releaseStrategy,
			ObjectProvider<MessageGroupProcessor> messageGroupProcessor,
			ObjectProvider<MessageGroupStore> messageStore) {
		AggregatorFactoryBean aggregator = new AggregatorFactoryBean();
		aggregator.setOutputChannelName(Processor.OUTPUT);
		aggregator.setExpireGroupsUponCompletion(true);
		aggregator.setSendPartialResultOnExpiry(true);
		aggregator.setGroupTimeoutExpression(this.properties.getGroupTimeout());

		aggregator.setCorrelationStrategy(correlationStrategy.getIfAvailable());
		aggregator.setReleaseStrategy(releaseStrategy.getIfAvailable());


		MessageGroupProcessor groupProcessor = messageGroupProcessor.getIfAvailable();

		if (groupProcessor == null) {
			groupProcessor = new DefaultAggregatingMessageGroupProcessor();
			((BeanFactoryAware) groupProcessor).setBeanFactory(this.beanFactory);
		}
		aggregator.setProcessorBean(groupProcessor);

		aggregator.setMessageStore(messageStore.getIfAvailable());

		return aggregator;
	}

	@Bean
	@ConditionalOnProperty(prefix = AggregatorProperties.PREFIX, name = "correlation")
	@ConditionalOnMissingBean
	public CorrelationStrategy correlationStrategy() {
		return new ExpressionEvaluatingCorrelationStrategy(this.properties.getCorrelation());
	}

	@Bean
	@ConditionalOnProperty(prefix = AggregatorProperties.PREFIX, name = "release")
	@ConditionalOnMissingBean
	public ReleaseStrategy releaseStrategy() {
		return new ExpressionEvaluatingReleaseStrategy(this.properties.getRelease().getExpressionString());
	}

	@Bean
	@ConditionalOnProperty(prefix = AggregatorProperties.PREFIX, name = "aggregation")
	@ConditionalOnMissingBean
	public MessageGroupProcessor messageGroupProcessor() {
		return new ExpressionEvaluatingMessageGroupProcessor(this.properties.getAggregation().getExpressionString());
	}


	@Configuration
	@ConditionalOnMissingBean(MessageGroupStore.class)
	@Import({ MessageStoreConfiguration.Mongo.class, MessageStoreConfiguration.Redis.class,
			MessageStoreConfiguration.Gemfire.class, MessageStoreConfiguration.Jdbc.class })
	protected static class MessageStoreAutoConfiguration {

	}

}
