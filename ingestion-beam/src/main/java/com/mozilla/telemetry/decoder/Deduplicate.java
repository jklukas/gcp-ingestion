/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.decoder;

import com.mozilla.telemetry.transforms.MapElementsWithErrors;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.UUID;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.joda.time.Duration;
import redis.clients.jedis.Jedis;

/**
 * Base class for deduplicating messages.
 *
 * <p>Subclasses are provided via static members removeDuplicates() and markAsSeen().
 *
 * <p>The packaging of subclasses here follows the style guidelines as captured in
 * https://beam.apache.org/contribute/ptransform-style-guide/#packaging-a-family-of-transforms
 */
public abstract class Deduplicate extends MapElementsWithErrors.ToPubsubMessageFrom<PubsubMessage> {
  private final ValueProvider<URI> uri;
  private transient Jedis jedis;

  Deduplicate(ValueProvider<URI> uri) {
    this.uri = uri;
  }

  /**
   * Lazy get transient {@link Jedis} client.
   */
  Jedis getJedis() {
    if (jedis == null) {
      jedis = new Jedis(uri.get());
    }
    return jedis;
  }

  /**
   * Get {@code document_id} attribute from {@link PubsubMessage} as {@code byte[]}.
   *
   * @exception IllegalArgumentException if {@code document_id} is an invalid {@link UUID}.
   */
  static Optional<byte[]> getId(PubsubMessage element) {
    return Optional
        .ofNullable(element.getAttributeMap())
        .flatMap(m -> Optional.ofNullable(m.get("document_id")))
        .map(UUID::fromString)
        .map(id -> ByteBuffer
            .wrap(new byte[16])
            .putLong(id.getLeastSignificantBits())
            .putLong(id.getMostSignificantBits())
            .array());
  }

  /*
   * Static factory methods.
   */

  public static Deduplicate removeDuplicates(ValueProvider<URI> uri) {
    return new RemoveDuplicates(uri);
  }

  public static Deduplicate removeDuplicates(URI uri) {
    return new RemoveDuplicates(StaticValueProvider.of(uri));
  }

  public static Deduplicate markAsSeen(ValueProvider<URI> uri, ValueProvider<Duration> ttl) {
    return new MarkAsSeen(uri, ttl);
  }

  public static Deduplicate markAsSeen(URI uri, Duration ttl) {
    return markAsSeen(StaticValueProvider.of(uri), StaticValueProvider.of(ttl));
  }

  /*
   * Concrete subclasses.
   */

  /**
   * {@link PTransform} that redirects messages already seen to {@code errorTag}.
   */
  private static class RemoveDuplicates extends Deduplicate {
    private RemoveDuplicates(ValueProvider<URI> uri) {
      super(uri);
    }

    private static class DuplicateIdException extends Exception {}

    @Override
    protected PubsubMessage processElement(PubsubMessage element) throws DuplicateIdException {
      // Throws IllegalArgumentException if id is present and invalid
      if (getId(element)
          // Throws JedisConnectionException if redis can't be reached
          .filter(getJedis()::exists)
          .isPresent()) {
        // Throw DuplicateIdException if id was in redis
        throw new DuplicateIdException();
      }
      return element;
    }
  }

  /**
   * {@link PTransform} to mark messages as seen, so any duplicates are removed.
   */
  public static class MarkAsSeen extends Deduplicate {
    Integer ttlSeconds;
    final ValueProvider<Duration> ttl;

    int getTtlSeconds() {
      if (ttlSeconds == null) {
        ttlSeconds = (int) ttl.get().getStandardSeconds();
      }
      return ttlSeconds;
    }

    private MarkAsSeen(ValueProvider<URI> uri, ValueProvider<Duration> ttl) {
      super(uri);
      this.ttl = ttl;
    }

    private String setex(byte[] id) {
      return getJedis().setex(id, getTtlSeconds(), new byte[0]);
    }

    @Override
    protected PubsubMessage processElement(PubsubMessage element) {
      // Throws IllegalArgumentException if id is present and invalid
      getId(element)
          // Throws JedisConnectionException if redis can't be reached
          .map(this::setex);
      return element;
    }
  }
}