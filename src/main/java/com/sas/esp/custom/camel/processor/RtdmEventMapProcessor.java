package com.sas.esp.custom.camel.processor;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;

public class RtdmEventMapProcessor implements Processor  {

	private int version = 1;
	private String clientTimeZone = null;

	private String correlationIdField = null;
	private String clientTimeZoneField = null;

	private String keep = null;
	private String drop = null;
	private Properties rename = null;

	List<String> keepFields = new ArrayList<String>();
	List<String> dropFields = new ArrayList<String>();



	@SuppressWarnings("unchecked")
	@Override
	public void process(Exchange exchange) throws Exception {

		// the body should be a map
		Map<String, Object> inMessage;

		// raise exception if it is not
		try {
			// FIXME as we are using multicast, we shoudn't need to do this
			inMessage = new LinkedHashMap<String, Object>(((Map<String, Object>) exchange.getIn().getBody()));
		} catch (Exception e) {
			throw new Exception("message body is or wrong type, expected Map<String, Object>");
		}


		Map<String, Object> eventMap = new LinkedHashMap<String, Object>();

		// fill the header
		// version
		eventMap.put("version", version);

		// if no field was set, fallback to exchangeId
		if (correlationIdField != null) {
			eventMap.put("correlationId", inMessage.get(correlationIdField));
		} else {
			eventMap.put("correlationId", exchange.getExchangeId());
		}

		// try to use the timeZone, then timeZoneField, then throw Exception
		if (clientTimeZone != null) {
			eventMap.put("clientTimeZone", clientTimeZone);
			// else
		} else if (clientTimeZoneField != null) {
			eventMap.put("clientTimeZone", inMessage.get(clientTimeZoneField));
		} else {
			throw new Exception("clientTimeZone or clientTimeZoneField must be set");
		}


		// Map to hold the result
		Map<String, Object> inputsMap = new LinkedHashMap<String, Object>();

		// Rename the fields if applicable
		if (rename != null) {
			for (Map.Entry<Object, Object> field : rename.entrySet())  {
				inMessage.put(field.getValue().toString(), inMessage.remove(field.getKey().toString()));
			}
		}

		// map the fields
		// if keep is supplied, map the fields based on its value - will also reorder the fields
		if (keepFields.size() > 0) {

			for (String keepField : keepFields) {
				inputsMap.put(keepField, inMessage.get(keepField));
			}

		} else {

			// if drop is supplied, remove the drop fields from the map
			if (dropFields.size() > 0) {

				for (String dropField : dropFields) {
					inMessage.remove(dropField);
				}

			}

			// loop through all the fields and get all the relevant ones, ignoring the headers
			for (Map.Entry<String, Object> field : inMessage.entrySet()) {

				// do not add the header fields
				if (!field.getKey().equals(correlationIdField) && !field.getKey().equals(clientTimeZoneField)) {
					inputsMap.put(field.getKey(), field.getValue());
				}

			}

		}


		eventMap.put("inputs", inputsMap);

		// output the created event
		exchange.getIn().setBody(eventMap);

	}


	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public String getClientTimeZone() {
		return clientTimeZone;
	}

	public void setClientTimeZone(String clientTimeZone) {
		this.clientTimeZone = clientTimeZone;
	}

	public String getCorrelationIdField() {
		return correlationIdField;
	}

	public void setCorrelationIdField(String correlationIdField) {
		this.correlationIdField = correlationIdField;
	}

	public String getClientTimeZoneField() {
		return clientTimeZoneField;
	}

	public void setClientTimeZoneField(String clientTimeZoneField) {
		this.clientTimeZoneField = clientTimeZoneField;
	}

	public String getKeep() {
		return keep;
	}

	public void setKeep(String keep) {
		this.keep = keep;
		dropFields.clear();

		for (String field : keep.split(" ")) {
			keepFields.add(field);
		}


	}

	public String getDrop() {
		return drop;
	}

	public void setDrop(String drop) {
		this.drop = drop;
		keepFields.clear();

		for (String field : drop.split(" ")) {
			dropFields.add(field);
		}
	}

	public Properties getRename() {
		return rename;
	}

	public void setRename(Properties rename) {
		this.rename = rename;
	}



}
