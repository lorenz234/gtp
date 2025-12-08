




    @task()
    def refresh_oli_tags(): # not needed anymore imo, needed but not here in OLI airtable DAG!
        """
        This task adds new oli tags to oli_tags table, only addative, doesn't remove tags!
        """
        from src.db_connector import DbConnector
        from src.misc.helper_functions import get_all_oli_tags_from_github

        # get tags from gtp-dna Github
        df = get_all_oli_tags_from_github()

        # upsert/update oli_tags table 
        db_connector = DbConnector()
        df = df.set_index('tag_id')
        db_connector.upsert_table('oli_tags', df, if_exists='update')


           @task()
    def refresh_trusted_entities(): # not needed anymore imo
        """
        This task gets the trusted entities from the gtp-dna Github and upserts them to the oli_trusted_entities table.
        """

        from src.misc.helper_functions import get_trusted_entities
        from src.db_connector import DbConnector
        db_connector = DbConnector()

        # get trusted entities from gtp-dna Github, rows with '*' are expanded based on public.oli_tags
        df = get_trusted_entities(db_connector)

        # turn attester into bytea
        df['attester'] = df['attester'].apply(lambda x: '\\x' + x[2:])

        # set attester & tag_id as index
        df = df.set_index(['attester', 'tag_id'])

        # upsert to oli_trusted_entities table, making sure to delete first for full refresh
        db_connector.delete_all_rows('oli_trusted_entities')
        db_connector.upsert_table('oli_trusted_entities', df)