package com.cloudsigma.flume.twitter;


import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;

public class TwitterSource extends AbstractSource implements EventDrivenSource, Configurable {

	private static final Logger logger = LoggerFactory.getLogger(TwitterSource.class);

	/** Information necessary for accessing the Twitter API */
	private String consumerKey;
	private String consumerSecret;
	private String accessToken;
	private String accessTokenSecret;

	private int count_prev_tweets;
	private String[] keywords;
	private String[] language;
	private long[] follow_ids;
	private double[][] locations;

	/** The actual Twitter stream. It's set up to collect raw JSON data */
	private TwitterStream twitterStream;

	/**
	 * The initialization method for the Source. The context contains all the Flume
	 * configuration info, and can be used to retrieve any configuration values
	 * necessary to set up the Source.
	 */
	public void configure(Context context) {
		consumerKey = context.getString(TwitterSourceConstants.CONSUMER_KEY_KEY);
		consumerSecret = context.getString(TwitterSourceConstants.CONSUMER_SECRET_KEY);
		accessToken = context.getString(TwitterSourceConstants.ACCESS_TOKEN_KEY);
		accessTokenSecret = context.getString(TwitterSourceConstants.ACCESS_TOKEN_SECRET_KEY);

		String keywordString = context.getString(TwitterSourceConstants.KEYWORDS_KEY, "");
		if (keywordString.trim().length() == 0) {
			keywords = new String[0];
		} else {
			keywords = keywordString.split(",");
			for (int i = 0; i < keywords.length; i++) {
				keywords[i] = keywords[i].trim();
			}
		}

		String languageString = context.getString(TwitterSourceConstants.LANGUAGE_KEY, "");
		if (languageString.trim().length() == 0) {
			language = new String[0];
		} else {
			language = languageString.split(",");
			for (int i = 0; i < language.length; i++) {
				language[i] = language[i].trim();
			}
		}

		String follow_ids_tmp = context.getString(TwitterSourceConstants.FOLLOW_IDS_KEY, "");

		if (follow_ids_tmp.length() == 0) {
			follow_ids = new long[0];
		} else {
			follow_ids = new long[follow_ids_tmp.length()];
			
			String follow_id_tmp[] = follow_ids_tmp.split(",");
			for (int i = 0; i < follow_id_tmp.length; i++) {
				follow_ids[i] = Long.parseLong(follow_id_tmp[i].trim());
			}

		}

		String locationstmp = context.getString(TwitterSourceConstants.LOCATIONS_KEY, "");
		if (locationstmp.trim().length() == 0) {
			locations = new double[0][0];
		} else {
			
			double[][] list;
			//[-179.231086,13.182335,179.859685,71.434357]
			String[] locationsArr = locationstmp.split(",");
			list = new double[locationsArr.length / 2][2];
			// 2D Array

			for (int i = 0; i < locationsArr.length; i++) {
				double[] tmp = new double[2];
				tmp[0] = Double.parseDouble(locationsArr[i]);
				tmp[1] = Double.parseDouble(locationsArr[i + 1]);

				list[i / 2] = tmp;

				i += 1;
			}
			locations = list;
		}

		String count_prev = context.getString(TwitterSourceConstants.COUNT_PREV_TWEETS_KEY, "");
		if (count_prev.trim().length() == 0) {
			count_prev_tweets = new Integer(0);
		} else {
			count_prev_tweets = Integer.parseInt(count_prev);
		}

		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setOAuthConsumerKey(consumerKey);
		cb.setOAuthConsumerSecret(consumerSecret);
		cb.setOAuthAccessToken(accessToken);
		cb.setOAuthAccessTokenSecret(accessTokenSecret);
		cb.setJSONStoreEnabled(true);
		cb.setIncludeEntitiesEnabled(true);

		twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
	}

	/**
	 * Start processing events. This uses the Twitter Streaming API to sample
	 * Twitter, and process tweets.
	 */
	
	@Override
	public void start() {
		// The channel is the piece of Flume that sits between the Source and Sink,
		// and is used to process events.
		final ChannelProcessor channel = getChannelProcessor();

		final Map<String, String> headers = new HashMap<String, String>();

		// The StatusListener is a twitter4j API, which can be added to a Twitter
		// stream, and will execute methods every time a message comes in through
		// the stream.
		StatusListener listener = new StatusListener() {
			// The onStatus method is executed every time a new tweet comes in.
			public void onStatus(Status status) {
				// The EventBuilder is used to build an event using the headers and
				// the raw JSON of a tweet
				// shouldn't log possibly sensitive customer data
				logger.debug("tweet arrived");

				headers.put("timestamp", String.valueOf(status.getCreatedAt().getTime()));
				Event event = EventBuilder.withBody(DataObjectFactory.getRawJSON(status).getBytes(), headers);

				channel.processEvent(event);
			}

			// This listener will ignore everything except for new tweets
			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
			}

			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
			}

			public void onScrubGeo(long userId, long upToStatusId) {
			}

			public void onException(Exception ex) {
			}

			public void onStallWarning(StallWarning warning) {
			}
		};

		logger.debug("Setting up Twitter sample stream using consumer key {} and" + " access token {}",
				new String[] { consumerKey, accessToken });
		// Set up the stream's listener (defined above),
		twitterStream.addListener(listener);

		// Set up a filter to pull out industry-relevant tweets

		if (keywords.length == 0 && count_prev_tweets == 0 && language.length == 0 && follow_ids.length == 0
				&& locations.length == 0) {
			logger.debug("Starting up Twitter sampling...");
			twitterStream.sample();
		} else {
			logger.debug("Starting up Twitter filtering...");

			FilterQuery query = new FilterQuery();

			if (keywords.length > 0) {
				query.track(keywords);
			}
			if (count_prev_tweets > 0) {
				query.count(count_prev_tweets);
			}
			if (language.length > 0) {
				query.language(language);
			}
			if (follow_ids.length > 0) {
				query.follow(follow_ids);
			}
			if (locations.length > 0) {
				query.locations(locations);
			}

			twitterStream.filter(query);

		}
		super.start();
	}

	/**
	 * Stops the Source's event processing and shuts down the Twitter stream.
	 */
	@Override
	public void stop() {
		logger.debug("Shutting down Twitter sample stream...");
		twitterStream.shutdown();
		super.stop();
	}
}
