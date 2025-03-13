from dotenv import dotenv_values
from converter.neo4jConnector import Neo4jConnector
from converter.Ifc2GraphTranslator import IFCGraphGenerator


def run_translation():
    # main entry point into converter

    # load config
    config = dotenv_values(".env")
    file_file_path = config["IFC-PATH"]

    # parse ifc file
    # set config=None if default values should be used
    connector = Neo4jConnector(config=config)
    connector.connect_driver()

    graph_generator = IFCGraphGenerator(
        connector, file_file_path, write_to_file=True)
    graph_generator.generateGraph()


if __name__ == "__main__":
    run_translation()
