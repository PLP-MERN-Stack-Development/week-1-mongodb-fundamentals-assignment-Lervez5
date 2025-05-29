const { MongoClient } = require('mongodb');

const uri = 'mongodb://localhost:27017';
const dbName = 'plp_bookstore';
const collectionName = 'books';

async function run() {
 const client = new MongoClient(uri);

 try {
    await client.connect();
    const db = client.db(dbName);
    const collection = db.collection(collectionName);
    console.log('Connected to MongoDB');

    console.log('\n Task 2: Basic Crud operations');

    // Find all books in the 'fiction' genre
    const fictionBooks = await collection.find({ genre: 'fiction' }).toArray();
    console.log('Fiction Books:', fictionBooks);

    // Find books published after 1951
    const booksAfter1951 = await collection.find({ published_Year: { $gt: 1951 } }).toArray();
    console.log('Books Published After 1951:', booksAfter1951);

    // Find books by George Orwell
    const orwellBooks = await collection.find({ author: 'George Orwell' }).toArray();
    console.log('Books by George Orwell:', orwellBooks);

    // Updates price of '1984' to $13.49
    const updatePrice = await collection.updateOne(
      { title: '1984' },
      { $set: { price: 13.49 } }
    );
    console.log(`\nUpdated 1984: ${updatePrice.modifiedCount} document(s) modified`);

    // Delete the book 'Moby Dick'
    const deleteResult = await collection.deleteOne({ title: 'Moby Dick' });
    console.log(`\nDeleted Moby Dick: ${deleteResult.deletedCount} document(s) removed`);

    console.log('\n TASK 3: ADVANCED QUERIES');

    // Find books that are in stock and published after 2010
    const recentStock = await collection.find({
      in_stock: true,
      published_year: { $gt: 2010 }
    }).toArray();
    console.log('\nIn-stock books published after 2010:\n', recentStock);

    // Projection: title, author, price
    const projected = await collection.find(
      {},
      { projection: { _id: 0, title: 1, author: 1, price: 1 } }
    ).toArray();
    console.log('\nProjected fields:\n', projected);

    // Sorting by price ascending
    const sortedAsc = await collection.find().sort({ price: 1 }).toArray();
    console.log('\nBooks sorted by price (low to high):\n', sortedAsc);

    // Sorting by price descending
    const sortedDesc = await collection.find().sort({ price: -1 }).toArray();
    console.log('\nBooks sorted by price (high to low):\n', sortedDesc);

    // Pagination: 5 books per page (Page 1)
    const page1 = await collection.find().skip(0).limit(5).toArray();
    console.log('\nPage 1 (first 5 books):\n', page1);

    // Pagination: Page 2
    const page2 = await collection.find().skip(5).limit(5).toArray();
    console.log('\nPage 2 (next 5 books):\n', page2);

    console.log('\n TASK 4: AGGREGATION PIPELINES');

    // Average price by genre
    const avgByGenre = await collection.aggregate([
      { $group: { _id: '$genre', avgPrice: { $avg: '$price' } } }
    ]).toArray();
    console.log('\nAverage price by genre:\n', avgByGenre);

    // Author with most books
    const topAuthor = await collection.aggregate([
      { $group: { _id: '$author', totalBooks: { $sum: 1 } } },
      { $sort: { totalBooks: -1 } },
      { $limit: 1 }
    ]).toArray();
    console.log('\nAuthor with the most books:\n', topAuthor);

    // Groups books by decade
    const byDecade = await collection.aggregate([
      {
        $addFields: {
          decade: {
            $concat: [
              { $toString: { $multiply: [{ $floor: { $divide: ['$published_year', 10] } }, 10] } },
              's'
            ]
          }
        }
      },
      {
        $group: {
          _id: '$decade',
          count: { $sum: 1 }
        }
      },
      { $sort: { _id: 1 } }
    ]).toArray();
    console.log('\nBooks grouped by publication decade:\n', byDecade);

    console.log('\n TASK 5: INDEXING');

    // Create index on title
    await collection.createIndex({ title: 1 });
    console.log('\nIndex created on title');

    // Create compound index on author + published_year
    await collection.createIndex({ author: 1, published_year: 1 });
    console.log('Compound index created on author + published_year');

    // Explain query on title
    const explainTitle = await collection.find({ title: '1984' }).explain('executionStats');
    console.log('\nExplain - search by title:\n', explainTitle.executionStats);

    // Explain query on compound index
    const explainCompound = await collection.find({
      author: 'George Orwell',
      published_year: 1949
    }).explain('executionStats');
    console.log('\nExplain - compound search:\n', explainCompound.executionStats);

  } catch (err) {
    console.error('❌ Error:', err);
  } finally {
    await client.close();
    console.log('MongoDB connection closed');
  }
}

run();