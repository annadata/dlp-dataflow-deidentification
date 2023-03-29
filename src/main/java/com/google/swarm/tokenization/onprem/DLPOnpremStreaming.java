package com.google.swarm.tokenization.onprem;


import com.google.api.services.bigquery.model.TableRow;
import com.google.swarm.tokenization.common.CSVFileReaderSplitDoFn;
import com.google.swarm.tokenization.common.ExtractColumnNamesTransform;
import com.google.swarm.tokenization.common.Util;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.*;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DLPOnpremStreaming {
    public static final Logger LOG = LoggerFactory.getLogger(DLPOnpremStreaming.class);
    private static final Duration DEFAULT_POLL_INTERVAL = Duration.standardSeconds(3);
    private static final Duration WINDOW_INTERVAL = Duration.standardSeconds(3);

    public static void main(String args[]) {
        System.out.println("Welcome !!!");
        DLPOnpremStreamingPipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).as(DLPOnpremStreamingPipelineOptions.class);
        run(options);
    }

    private static void run(DLPOnpremStreamingPipelineOptions options) {
        Pipeline p = Pipeline.create(options);
        PCollection<KV<String, FileIO.ReadableFile>> inputFiles =
                p
                        /*
                         * 1) Read from the text source continuously based on default interval e.g. 300 seconds
                         *     - Setup a window for 30 secs to capture the list of files emited.
                         *     - Group by file name as key and ReadableFile as a value.
                         */
                        .apply(
                                "Poll Input Files",
                                FileIO.match()
                                        .filepattern(options.getInputFilePattern())
                                        .continuously(DEFAULT_POLL_INTERVAL, Watch.Growth.never()))
                        .apply("Find Pattern Match", FileIO.readMatches().withCompression(Compression.AUTO))
                        .apply("Add File Name as Key", WithKeys.of(file -> file.getMetadata().resourceId().getFilename().toString()))
                        .setCoder(KvCoder.of(StringUtf8Coder.of(), ReadableFileCoder.of()))
                        .apply(
                                "Fixed Window(30 Sec)",
                                Window.<KV<String, FileIO.ReadableFile>>into(FixedWindows.of(WINDOW_INTERVAL))
                                        .triggering(
                                                Repeatedly.forever(
                                                        AfterProcessingTime.pastFirstElementInPane()
                                                                .plusDelayOf(Duration.ZERO)))
                                        .discardingFiredPanes()
                                        .withAllowedLateness(Duration.ZERO))
                        .apply(GroupByKey.create())
                        .apply(
                                "File Handler",
                                ParDo.of(
                                        new DoFn<KV<String, Iterable<FileIO.ReadableFile>>, KV<String, FileIO.ReadableFile>>() {
                                            @ProcessElement
                                            public void processElement(ProcessContext c) {
                                                String fileKey = c.element().getKey();
                                                c.element()
                                                        .getValue()
                                                        .forEach(
                                                                file -> {
                                                                    c.output(KV.of(fileKey, file));
                                                                });
                                            }
                                        }));

        final PCollectionView<Map<String, List<String>>> headers =
                inputFiles.apply(
                        "Extract Column Names",
                        ExtractColumnNamesTransform.newBuilder()
                                // TODO get rid of hardcoding to support other file formats in future.
                                .setFileType(Util.FileType.CSV)
                                .setHeaders(Collections.<String>emptyList())
                                .build());

        PCollection<KV<String, String>> records =
                inputFiles
                        .apply(
                                "SplitCSVFile",
                                ParDo.of(
                                        new CSVFileReaderSplitDoFn(
                                                options.getRecordDelimiter(), options.getSplitSize())))
                        .apply(ParDo.of( new DLPTransformPlaceholderDoFn()));

        records.apply("Write to CSV", FileIO.<String, KV<String, String>>writeDynamic()
                .by(KV::getKey)
                .withDestinationCoder(StringUtf8Coder.of())
                .via(Contextful.fn(KV::getValue), TextIO.sink())
                .to(options.getOutputDirectory())
                .withNaming(key -> new CsvFileNaming(key))
                .withNumShards(1));


        inputFiles.apply("Print file names", ParDo.of(new DoFn<KV<String, FileIO.ReadableFile>, KV<String, FileIO.ReadableFile>>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                System.out.println("******** FileName: " + c.element().getKey());

                LOG.info("******** FileName: " + c.element().getKey());
            }
        }));

        p.run();
    }

    private static class DLPTransformPlaceholderDoFn  extends DoFn<KV<String, String>, KV<String, String>> {
        @ProcessElement
        public void processElement(ProcessContext c) throws IOException {
            String fileName = c.element().getKey();
            String line = c.element().getValue();
            DLPOnpremStreamingPipelineOptions options = c.getPipelineOptions().as(DLPOnpremStreamingPipelineOptions.class);
            List<String> columnVals = Arrays.asList(line.split(options.getColumnDelimiter().toString()));
            // Cheap simulation of de-id of CC
            columnVals.set(4,"****************");
            StringBuffer lineBuf = new StringBuffer();
            // Quick and dirty!
            for (String colVal:columnVals) {
                if (lineBuf.length() == 0) {
                    lineBuf.append(colVal);
                } else {
                    lineBuf.append(",").append(colVal);
                }
            }
            c.output(KV.of(fileName, lineBuf.toString()));
<<<<<<< HEAD
            LOG.info("KV.of("+fileName+" , "+lineBuf.toString()+")");
=======
>>>>>>> 56012d2 (WIP - Output to CSV files)
        }
    }

    public static class CsvFileNaming implements FileIO.Write.FileNaming {
        private String fileName;
        public CsvFileNaming(String fileName) {
            this.fileName = fileName;
        }

        @Override
        public @UnknownKeyFor @NonNull @Initialized String getFilename(@UnknownKeyFor @NonNull @Initialized BoundedWindow window, @UnknownKeyFor @NonNull @Initialized PaneInfo pane, @UnknownKeyFor @NonNull @Initialized int numShards, @UnknownKeyFor @NonNull @Initialized int shardIndex, @UnknownKeyFor @NonNull @Initialized Compression compression) {
            return fileName;
        }
    }


}
