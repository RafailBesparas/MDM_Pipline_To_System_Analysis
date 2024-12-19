from pyspark.sql.functions import struct, lit, col, array, when, isnull, filter, current_timestamp, date_format, expr, \
    collect_list

# creates a triplet of three values Operation, new value, old value
# We must send all data elements in triplets
#
def get_insert_operation(column, alias):
    return struct(lit("INSERT").alias("operation"),
                  column.alias("newValue"),
                  lit(None).alias("oldValue")).alias(alias)

# We have three tranformation functions for these three tables
# get contract, get relations, get_address

#Main purpose of funtions is to take an account dataframe
#Convert it into a dataframe of triplets for account fields
#We have the null handling complexities for the fields
#
def get_contract(df):
    contract_title = array(when(~isnull("legal_title_1"),
                                struct(lit("lgl_ttl_ln_1").alias("contractTitleLineType"),
                                       col("legal_title_1").alias("contractTitleLine")).alias("contractTitle")),
                           when(~isnull("legal_title_2"),
                                struct(lit("lgl_ttl_ln_2").alias("contractTitleLineType"),
                                       col("legal_title_2").alias("contractTitleLine")).alias("contractTitle"))
                           )

    contract_title_nl = filter(contract_title, lambda x: ~isnull(x))

    tax_identifier = struct(col("tax_id_type").alias("taxIdType"),
                            col("tax_id").alias("taxId")).alias("taxIdentifier")

    return df.select("account_id", get_insert_operation(col("account_id"), "contractIdentifier"),
                     get_insert_operation(col("source_sys"), "sourceSystemIdentifier"),
                     get_insert_operation(col("account_start_date"), "contactStartDateTime"),
                     get_insert_operation(contract_title_nl, "contractTitle"),
                     get_insert_operation(tax_identifier, "taxIdentifier"),
                     get_insert_operation(col("branch_code"), "contractBranchCode"),
                     get_insert_operation(col("country"), "contractCountry"),
                     )

# Will take the party dataframe and create triples for the approprate fields.
def get_relations(df):
    return df.select("account_id", "party_id",
                     get_insert_operation(col("party_id"), "partyIdentifier"),
                     get_insert_operation(col("relation_type"), "partyRelationshipType"),
                     get_insert_operation(col("relation_start_date"), "partyRelationStartDateTime")
                     )


# Get Address takes an address dataframe and packages all the fields into the party address struct.
# We use the get_instert to convert partyAddress to the triplet of operation, oldvalue, newvalue

def get_address(df):
    address = struct(col("address_line_1").alias("addressLine1"),
                     col("address_line_2").alias("addressLine2"),
                     col("city").alias("addressCity"),
                     col("postal_code").alias("addressPostalCode"),
                     col("country_of_address").alias("addressCountry"),
                     col("address_start_date").alias("addressStartDate")
                     )

    return df.select("party_id", get_insert_operation(address, "partyAddress"))

# We have the functions to join the dataframes

# Left outer join
# Complex: Collect multiple parties for a single account and create a list
# We group on account_id and use collect_list to create a list of all parties by the account ID
#
def join_party_address(p_df, a_df):
    return p_df.join(a_df, "party_id", "left_outer") \
        .groupBy("account_id") \
        .agg(collect_list(struct("partyIdentifier",
                                 "partyRelationshipType",
                                 "partyRelationStartDateTime",
                                 "partyAddress"
                                 ).alias("partyDetails")
                          ).alias("partyRelations"))

# Simple left join operation
def join_contract_party(c_df, p_df):
    return c_df.join(p_df, "account_id", "left_outer")

# It takes a final dataframe and add an event header to each record
#
def apply_header(spark, df):
    header_info = [("SBDL-Contract", 1, 0), ]
    header_df = spark.createDataFrame(header_info) \
        .toDF("eventType", "majorSchemaVersion", "minorSchemaVersion")

    event_df = header_df.hint("broadcast").crossJoin(df) \
        .select(struct(expr("uuid()").alias("eventIdentifier"),
                       col("eventType"), col("majorSchemaVersion"), col("minorSchemaVersion"),
                       lit(date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ssZ")).alias("eventDateTime")
                       ).alias("eventHeader"),
                array(struct(lit("contractIdentifier").alias("keyField"),
                             col("account_id").alias("keyValue")
                             )).alias("keys"),
                struct(col("contractIdentifier"),
                       col("sourceSystemIdentifier"),
                       col("contactStartDateTime"),
                       col("contractTitle"),
                       col("taxIdentifier"),
                       col("contractBranchCode"),
                       col("contractCountry"),
                       col("partyRelations")
                       ).alias("payload")
                )

    return event_df