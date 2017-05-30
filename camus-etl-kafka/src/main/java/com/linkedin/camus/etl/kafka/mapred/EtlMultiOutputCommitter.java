package com.linkedin.camus.etl.kafka.mapred;

import com.linkedin.camus.etl.Partitioner;
import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.kafka.common.EtlCounts;
import com.linkedin.camus.etl.kafka.common.EtlKey;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.linkedin.camus.etl.Partitioner.WORKING_FILE_PREFIX;


public class EtlMultiOutputCommitter extends FileOutputCommitter {

    private final RecordWriterProvider recordWriterProvider;
    private final Pattern workingFileMetadataPattern;
    private final HashMap<String, EtlCounts> counts = new HashMap<>();
    private final HashMap<String, EtlKey> offsets = new HashMap<>();
    private final HashMap<String, Long> eventCounts = new HashMap<>();
    private final TaskAttemptContext context;
    private final Logger log;

    public EtlMultiOutputCommitter(Path outputPath, TaskAttemptContext context, Logger log)
            throws IOException {
        super(outputPath, context);
        this.context = context;
        try {
            Class<RecordWriterProvider> rwp = EtlMultiOutputFormat
                    .getRecordWriterProviderClass(context);
            Constructor<RecordWriterProvider> crwp = rwp.getConstructor(TaskAttemptContext.class);
            recordWriterProvider = crwp.newInstance(context);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        workingFileMetadataPattern = Pattern.compile(
                 "\\.([^\\.]+)\\.([\\d_]+)\\.(\\d+)\\.([^\\.]+)-m-\\d+" + recordWriterProvider
                        .getFilenameExtension());
        this.log = log;
    }

    private void mkdirs(FileSystem fs, Path path) throws IOException {
        if (!fs.exists(path.getParent())) {
            mkdirs(fs, path.getParent());
        }
        fs.mkdirs(path);
    }

    public void addCounts(EtlKey key) throws IOException {
        String workingFileName = EtlMultiOutputFormat.getWorkingFileName(context, key);
        if (!counts.containsKey(workingFileName)) {
            counts.put(workingFileName,
                       new EtlCounts(key.getTopic(),
                                     EtlMultiOutputFormat.getMonitorTimeGranularityMs(context)));
        }
        counts.get(workingFileName).incrementMonitorCount(key);
        addOffset(key);
    }

    public void addOffset(EtlKey key) {
        String topicPart = key.getTopic() + "-" + key.getLeaderId() + "-" + key.getPartition();
        EtlKey offsetKey = new EtlKey(key);

        if (offsets.containsKey(topicPart)) {
            long totalSize = offsets.get(topicPart).getTotalMessageSize() + key.getMessageSize();
            long avgSize = totalSize / (eventCounts.get(topicPart) + 1);
            offsetKey.setMessageSize(avgSize);
            offsetKey.setTotalMessageSize(totalSize);
        } else {
            eventCounts.put(topicPart, 0L);
        }
        eventCounts.put(topicPart, eventCounts.get(topicPart) + 1);
        offsets.put(topicPart, offsetKey);
    }

    @Override
    public void commitTask(TaskAttemptContext context) throws IOException {

        ArrayList<Map<String, Object>> allCountObject = new ArrayList<>();
        FileSystem fs = FileSystem.get(context.getConfiguration());
        if (EtlMultiOutputFormat.isRunMoveData(context)) {
            Path workPath = super.getWorkPath();
            log.info("work path: " + workPath);
            Path baseOutDir = EtlMultiOutputFormat.getDestinationPath(context);
            log.info("Destination base path: " + baseOutDir);
            for (FileStatus f : fs.listStatus(workPath)) {
                String file = f.getPath().getName();
                log.info("work file: " + file);
                if (file.startsWith(WORKING_FILE_PREFIX)) {
                    String workingFileName = file.substring(0, file.lastIndexOf("-m"));
                    EtlCounts count = counts.get(workingFileName);
                    count.setEndTime(System.currentTimeMillis());

                    String partitionedFile =
                            getPartitionedPath(context, file, count.getEventCount(),
                                               count.getLastKey().getOffset());

                    Path dest = new Path(baseOutDir, partitionedFile);

                    if (!fs.exists(dest.getParent())) {
                        mkdirs(fs, dest.getParent());
                    }

                    commitFile(fs, f.getPath(), dest);
                    log.info("Moved file from: " + f.getPath() + " to: " + dest);

                    if (EtlMultiOutputFormat.isRunTrackingPost(context)) {
                        count.writeCountsToMap(allCountObject);
                    }
                }
            }

            if (EtlMultiOutputFormat.isRunTrackingPost(context)) {
                Path tempPath = new Path(workPath, "counts." + context.getConfiguration()
                        .get("mapred.task.id"));
                OutputStream outputStream = new BufferedOutputStream(fs.create(tempPath));
                ObjectMapper mapper = new ObjectMapper();
                log.info("Writing counts to : " + tempPath.toString());
                long time = System.currentTimeMillis();
                mapper.writeValue(outputStream, allCountObject);
                log.debug("Time taken : " + (System.currentTimeMillis() - time) / 1000);
            }
        } else {
            log.info("Not moving run data.");
        }

        SequenceFile.Writer offsetWriter =
                SequenceFile.createWriter(fs, context.getConfiguration(),
                                          new Path(super.getWorkPath(),
                                                   EtlMultiOutputFormat.getUniqueFile(
                                                           context,
                                                           EtlMultiOutputFormat.OFFSET_PREFIX,
                                                           "")),
                                          EtlKey.class,
                                          NullWritable.class);
        for (String s : offsets.keySet()) {
            log.info("Avg record size for " + offsets.get(s).getTopic() + ":" + offsets.get(s)
                    .getPartition() + " = "
                     + offsets.get(s).getMessageSize());
            offsetWriter.append(offsets.get(s), NullWritable.get());
        }
        offsetWriter.close();
        super.commitTask(context);
    }

    protected void commitFile(FileSystem fs, Path source, Path target) throws IOException {
        log.info(String.format("Moving %s to %s", source, target));
        if (!fs.rename(source, target)) {
            log.error(String.format("Failed to move from %s to %s", source, target));
            throw new IOException(String.format("Failed to move from %s to %s", source, target));
        }
    }

    public String getPartitionedPath(JobContext context, String file, int count, long offset)
            throws IOException {
        final Matcher m = workingFileMetadataPattern.matcher(file);
        if (!m.find()) {
            throw new IOException(
                    "Could not extract metadata from working filename '" + file + "'");
        }
        String topic = m.group(1);
        String encodedPartition = m.group(4);

        final Partitioner partitioner = EtlMultiOutputFormat.getPartitioner(context, topic);
        String partitionedPath = partitioner.generatePartitionedPath(context, topic,
                                                                     encodedPartition);

        final int extensionIndex = file.lastIndexOf(recordWriterProvider.getFilenameExtension());

        return partitionedPath + "/" +
               file.substring(0, extensionIndex) +
               partitioner.generateFileName(count, offset) +
               recordWriterProvider.getFilenameExtension();
    }
}
