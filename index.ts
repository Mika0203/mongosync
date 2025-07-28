import { MongoClient, type MongoClientOptions } from "mongodb";

type ServerOptions = {
  /**
   * The URL of the MongoDB server.
   * Example: "mongodb://username:password@host:port/"
   */
  url: string;

  mongoOptions?: MongoClientOptions;
};

interface Types {
  /**
   * Options for the source MongoDB server.
   */
  from: ServerOptions;

  /**
   * Options for the destination MongoDB server.
   */
  to: ServerOptions;

  /**
   * The name of the database to sync.
   */
  dbName: string;

  /**
   * An optional name for the destination database.
   * If not provided, the same database name as `dbName` will be used.
   */
  destinationDbName?: string;

  /**
   * An optional array of collection names to sync. If not provided, all collections will be synced.
   */
  collections?: string[];
}

export async function sync({
  from,
  to,
  dbName,
  destinationDbName,
  collections = [],
}: Types) {
  const fromServer = new MongoClient(from.url, from.mongoOptions);
  const toServer = new MongoClient(to.url, to.mongoOptions);
  await fromServer.connect();
  await toServer.connect();
  console.log("Connected to both MongoDB servers");

  const fromDb = fromServer.db(dbName);
  const toDb = toServer.db(destinationDbName || dbName);

  const collectionNames = (await fromDb.listCollections().toArray()).filter(
    (name) =>
      collections && collections.length ? collections.includes(name.name) : true
  );

  if (collectionNames.length === 0) {
    console.log("No collections to sync");
    await fromServer.close();
    await toServer.close();
    return;
  }

  for (const collection of collectionNames) {
    console.log("----------------------------------------");
    console.log(`Syncing collection: [${collection.name}]`);

    const fromCollection = fromDb.collection(collection.name);
    if (!(await toDb.listCollections({ name: collection.name }).hasNext())) {
      await toDb.createCollection(collection.name);
      console.log(`Created collection: [${collection.name}]`);
    }

    const toCollection = toDb.collection(collection.name);
    const count = await fromCollection.countDocuments();
    let maxCount = count;
    const cursor = fromCollection.find({});
    const batchSize = 1000;
    let batch: any[] = [];

    for await (const doc of cursor) {
      batch.push(doc);
      if (batch.length === batchSize) {
        maxCount -= batch.length;
        console.log(`[${collection.name}] remaining : ${maxCount}`);
        await toCollection.insertMany(batch);
        batch = [];
      }
    }

    if (batch.length > 0) {
      await toCollection.insertMany(batch);
    }

    console.log(`Synced collection: [${collection.name}]`);
  }

  console.log("All collections synced successfully");
  await fromServer.close();
  await toServer.close();

  console.log("Connections closed");
}
