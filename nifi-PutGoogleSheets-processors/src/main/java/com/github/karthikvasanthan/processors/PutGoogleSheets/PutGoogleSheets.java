/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.karthikvasanthan.processors.PutGoogleSheets;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.AppendValuesResponse;
import com.google.api.services.sheets.v4.model.ValueRange;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.json.JSONArray;
import org.json.JSONException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Tags({"custom", "google-sheets", "sheets"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
public class PutGoogleSheets extends AbstractProcessor {

    public static final PropertyDescriptor JSON_CREDS = new PropertyDescriptor
            .Builder().name("Json Credentials")
            .description("Credentials for the service account that will write to google sheets")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SHEET_URL_ID = new PropertyDescriptor
            .Builder().name("Sheet url id")
            .description("The id of the google sheet (from the url)")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SHEET_NAME = new PropertyDescriptor
            .Builder().name("Sheet name")
            .description("The name of the sheet in the file to update")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully updated sheet data")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed to update sheets")
            .build();
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
    private static HttpTransport HTTP_TRANSPORT;
    protected volatile Sheets GSheets;
    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    public static boolean isValidJsonArray(String jsonString) {

        boolean isValid;

        try {
            JSONArray jsonArr = new JSONArray(jsonString);
            isValid = true;
        } catch (JSONException ex) {
            isValid = false;
        }
        return isValid;
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(JSON_CREDS);
        descriptors.add(SHEET_URL_ID);
        descriptors.add(SHEET_NAME);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        this.GSheets = CreateGSheets(context);
    }

    public Sheets CreateGSheets(ProcessContext context) throws IOException {
        PropertyValue serviceAccountCredentialsJsonProperty = context.getProperty(JSON_CREDS);
        try {
            HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(1);
        }

        GoogleCredential credential = GoogleCredential.fromStream(new ByteArrayInputStream(serviceAccountCredentialsJsonProperty.getValue().getBytes(StandardCharsets.UTF_8)))
                .createScoped(Collections.singleton(SheetsScopes.SPREADSHEETS));

        return new Sheets.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential)
                .build();

    }

    public Sheets getGSheets() {
        return GSheets;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String spreadsheetId = context.getProperty(SHEET_URL_ID).evaluateAttributeExpressions(flowFile).getValue();
        final String range = context.getProperty(SHEET_NAME).evaluateAttributeExpressions(flowFile).getValue();

        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        session.exportTo(flowFile, bytes);
        final String contents = bytes.toString();
        List<List<Object>> requestBody = new ArrayList<>();

        if (isValidJsonArray(contents)) {
            final ObjectMapper mapper = new ObjectMapper();
            try {
                List<LinkedHashMap<Object, Object>> flowFileMap = mapper.readValue(contents, new TypeReference<ArrayList<LinkedHashMap<Object, Object>>>() {
                });

                for (LinkedHashMap<Object, Object> json : flowFileMap) {
                    List<Object> data = new ArrayList<>();
                    for (Map.Entry<Object, Object> entry : json.entrySet()) {
                        data.add(entry.getValue() == null ? "null" : entry.getValue());
                    }
                    requestBody.add(data);
                }
            } catch (IOException e) {
                getLogger().error("IO Exception when processing json array");
            }

            ValueRange valueRange = new ValueRange();
            valueRange.setValues(requestBody);
            try {
                Sheets.Spreadsheets.Values.Append request = GSheets.spreadsheets().values().append(spreadsheetId, range, valueRange).setValueInputOption("RAW");
                AppendValuesResponse response = request.execute();
                flowFile = session.write(flowFile, new OutputStreamCallback() {
                    @Override
                    public void process(OutputStream outputStream) throws IOException {
                        outputStream.write(response.toPrettyString().getBytes());
                        //outputStream.write(requestBody.toString().getBytes());
                    }
                });
                session.transfer(flowFile, REL_SUCCESS);

            } catch (IOException e) {
                getLogger().error("IO Exception when writing to sheets: " + e.toString());
                session.transfer(flowFile, REL_FAILURE);
            }
        } else {
            getLogger().error("Data is not valid JSON array");
            session.transfer(flowFile, REL_FAILURE);
        }

        session.commit();

    }
}
