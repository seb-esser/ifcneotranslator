from dotenv import dotenv_values
from neo4j import GraphDatabase


class Neo4jConnector:
    """ handles the connection to a given neo4j database """

    my_driver = []

    # constructor
    def __init__(self, config=None):

        if config == None:
            # default values
            self.uri = "bolt:localhost:7687"
            self.user = "neo4j"
            self.password = "password"
        else:
            self.uri = config["NEO4J-URI"]
            self.user = config["NEO4J-USER"]
            self.password = config["NEO4J-PASSWORD"]

    # methods
    def connect_driver(self):
        """
        creates a new connection to the database
        @return:
        """
        try:
            self.my_driver = GraphDatabase.driver(
                self.uri, auth=(self.user, self.password))
        except self.my_driver:
            raise Exception("Oops!  Connection failed.  Try again...")

    def run_cypher_statement(self, statement, postStatement=None):
        """
        executes a given cypher statement and does some post processing if stated
        @statement: cypher command
        @postStatement: post processing of response
        @return
        """

        try:
            with self.my_driver.session() as session:
                with session.begin_transaction() as tx:
                    res = tx.run(statement)
                    return_val = []

                    if postStatement != None:
                        for record in res:
                            # print(record[postStatement])
                            return_val.append(record[postStatement])

                    else:
                        for record in res:
                            # print(record)
                            return_val.append(record)
                return return_val
        except:
            raise Exception('Error in neo4j Connector.')

    def disconnect_driver(self):
        """
        disconnects the connector instance
        @return:
        """
        self.my_driver.close()
