/*
 * (C) Copyright 2017 Nuxeo SA (http://nuxeo.com/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     bdelbosc
 */
package org.nuxeo.ecm.platform.importer.mqueues.pattern.producer;

import org.nuxeo.ecm.platform.importer.mqueues.pattern.message.BlobMessage;
import org.nuxeo.ecm.platform.importer.random.HunspellDictionaryHolder;
import org.nuxeo.ecm.platform.importer.random.RandomTextGenerator;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Build random StringBlob message.
 *
 * @since 9.1
 */
public class RandomStringBlobMessageProducer extends AbstractProducer<BlobMessage> {
    private static final String DEFAULT_MIME_TYPE = "plain/text";
    private final long nbBlobs;
    private final int averageSizeKB;
    private final ThreadLocalRandom rand;
    private long count = 0;
    private static RandomTextGenerator gen;
    private final String mimetype;

    public RandomStringBlobMessageProducer(int producerId, long nbBlobs, String lang, int averageSizeKB) {
        super(producerId);
        this.nbBlobs = nbBlobs;
        this.averageSizeKB = averageSizeKB;
        this.mimetype = DEFAULT_MIME_TYPE;
        synchronized (RandomDocumentMessageProducer.class) {
            if (gen == null) {
                gen = new RandomTextGenerator(new HunspellDictionaryHolder(lang));
                gen.prefilCache();
            }
        }
        rand = ThreadLocalRandom.current();
    }

    @Override
    public int getShard(BlobMessage message, int shards) {
        return ((int) count) % shards;
    }

    @Override
    public boolean hasNext() {
        return count < nbBlobs;
    }

    @Override
    public BlobMessage next() {
        String filename = generateFilename();
        String content = generateContent();
        BlobMessage ret = new BlobMessage.StringMessageBuilder(content)
                .setFilename(filename).setMimetype(mimetype).build();
        count++;
        return ret;
    }

    private String generateFilename() {
        return gen.getRandomTitle(rand.nextInt(4) + 1).trim().replaceAll("\\W+", "-").toLowerCase() + ".txt";
    }

    private String generateContent() {
        return gen.getRandomText(averageSizeKB);
    }


}
