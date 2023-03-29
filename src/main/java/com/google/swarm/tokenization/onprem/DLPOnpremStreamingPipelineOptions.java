package com.google.swarm.tokenization.onprem;


import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface DLPOnpremStreamingPipelineOptions extends PipelineOptions {

    @Validation.Required
    @Description("Input file patter. Example: /path/to/dir/*.csv")
    String getInputFilePattern();

    void setInputFilePattern(String inputDirectory);

    @Validation.Required
    String getOutputDirectory();

    void setOutputDirectory(String outputDirectory);

    @Validation.Required
    String getDLPServerUrl();

    void setDLPServerUrl(String dlpServerUrl);

    @Validation.Required
    String getDeidentifyConfigJson();

    void setDeidentifyConfigJson(String deidentifyConfigJsonFilePath);

    @Description("Record delimiter")
    @Default.String("\n")
    String getRecordDelimiter();

    void setRecordDelimiter(String value);

    @Default.Integer(900 * 1000)
    Integer getSplitSize();

    void setSplitSize(Integer value);

    @Description("Column delimiter")
    @Default.Character(',')
    Character getColumnDelimiter();

    void setColumnDelimiter(Character value);

}
