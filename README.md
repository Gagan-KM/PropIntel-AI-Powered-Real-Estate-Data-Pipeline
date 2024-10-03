# PropIntel-AI-Powered-Real-Estate-Data-Pipeline

![Uploading image.png…]()

This project is an end-to-end real estate data engineering pipeline that leverages AI and modern data technologies to extract, process, and store real-time property information.

The pipeline begins by gathering property data from Zoopla, a major online real estate platform. Using the Bright Data Web Scraper, property listings, images, prices, and other essential details are extracted. These details are processed by a Large Language Model (LLM) powered by OpenAI, which refines the data, performs transformations, and structures it into a more useful format.

Next, the data is streamed into Kafka, a distributed message broker, which handles real-time data ingestion and routing. The data is then processed by Apache Spark, which performs real-time stream processing, transforming and enriching the data before it's stored.

Finally, the processed data is stored in Cassandra, a scalable NoSQL database that allows for fast data retrieval, supporting future analysis or integration with various applications. This architecture is designed to handle the complexities of real estate data, providing scalable and efficient data processing and storage for analytics and decision-making.
