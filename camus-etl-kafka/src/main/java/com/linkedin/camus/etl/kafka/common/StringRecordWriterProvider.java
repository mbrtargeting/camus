package com.linkedin.camus.etl.kafka.common;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataOutputStream;
import java.io.IOException;


/**
 * Provides a RecordWriter that uses FSDataOutputStream to write
 * a String record as bytes to HDFS without any reformatting or compression.
 */
public class StringRecordWriterProvider implements RecordWriterProvider {

    public static final String ETL_OUTPUT_RECORD_DELIMITER = "etl.output.record.delimiter";
    public static final String DEFAULT_RECORD_DELIMITER = "\n";

    protected String recordDelimiter = null;

    private String extension = "";
    private boolean isCompressed = false;
    private CompressionCodec codec = null;

    public StringRecordWriterProvider(TaskAttemptContext context) {
        Configuration conf = context.getConfiguration();

        if (recordDelimiter == null) {
            recordDelimiter = conf.get(ETL_OUTPUT_RECORD_DELIMITER, DEFAULT_RECORD_DELIMITER);
        }

        isCompressed = FileOutputFormat.getCompressOutput(context);

        if (isCompressed) {
            // find the right codec
            final Class<? extends CompressionCodec> codecClass =
                    FileOutputFormat.getOutputCompressorClass(context, DefaultCodec.class);
            codec = ReflectionUtils.newInstance(codecClass, conf);
            extension = codec.getDefaultExtension();
        }
    }

    @Override
    public String getFilenameExtension() {
        return extension;
    }

    @Override
    public RecordWriter<IEtlKey, CamusWrapper> getDataRecordWriter(TaskAttemptContext context,
                                                                   String fileName,
                                                                   CamusWrapper camusWrapper,
                                                                   FileOutputCommitter committer)
            throws IOException {

        // If recordDelimiter hasn't been initialized, do so now
        if (recordDelimiter == null) {
            recordDelimiter = context.getConfiguration()
                    .get(ETL_OUTPUT_RECORD_DELIMITER, DEFAULT_RECORD_DELIMITER);
        }

        // Get the filename for this RecordWriter.
        Path path =
                new Path(committer.getWorkPath(), EtlMultiOutputFormat
                        .getUniqueFile(context, fileName, getFilenameExtension()));

        FileSystem fs = path.getFileSystem(context.getConfiguration());
        final FSDataOutputStream fileOut = fs.create(path, false);
        if (!isCompressed) {
            return new ByteRecordWriter(fileOut, recordDelimiter);
        } else {
            return new ByteRecordWriter(new DataOutputStream(codec.createOutputStream(fileOut)),
                                        recordDelimiter);
        }
    }

    protected static class ByteRecordWriter extends RecordWriter<IEtlKey, CamusWrapper> {

        private final DataOutputStream out;
        private final String recordDelimiter;

        public ByteRecordWriter(DataOutputStream out, String recordDelimiter) {
            this.out = out;
            this.recordDelimiter = recordDelimiter;
        }

        @Override
        public void write(IEtlKey ignore, CamusWrapper value) throws IOException {
            boolean nullValue = value == null;
            if (!nullValue) {
                String record = value.getRecord() + recordDelimiter;
                out.write(record.getBytes());
            }
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext)
                throws IOException, InterruptedException {
            out.close();
        }
    }
}
