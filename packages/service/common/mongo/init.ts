import { delay } from '@fastgpt/global/common/system/utils';
import { addLog } from '../system/log';
import type { Mongoose } from 'mongoose';

const maxConnecting = Math.max(30, Number(process.env.DB_MAX_LINK || 20));

/**
 * connect MongoDB and init data
 */
export async function connectMongo(props: {
  db: Mongoose;
  url: string;
  connectedCb?: () => void;
}): Promise<Mongoose> {
  const { db, url, connectedCb } = props;

  /* Connecting, connected will return */
  if (db.connection.readyState !== 0) {
    return db;
  }

  const RemoveListeners = () => {
    db.connection.removeAllListeners('error');
    db.connection.removeAllListeners('disconnected');
  };

  console.log('MongoDB start connect');
  try {
    // Remove existing listeners to prevent duplicates
    RemoveListeners();
    db.set('strictQuery', 'throw');

    db.connection.on('error', async (error) => {
      console.log('mongo error', error);
      try {
        if (db.connection.readyState !== 0) {
          RemoveListeners();
          await db.disconnect();
          await delay(1000);
          await connectMongo(props);
        }
      } catch (error) {}
    });
    db.connection.on('disconnected', async () => {
      console.log('mongo disconnected');
      try {
        if (db.connection.readyState !== 0) {
          RemoveListeners();
          await db.disconnect();
          await delay(1000);
          await connectMongo(props);
        }
      } catch (error) {}
    });

    await db.connect(url, {
      bufferCommands: true,
      maxConnecting: maxConnecting,
      maxPoolSize: maxConnecting,
      minPoolSize: (process.env.DB_MIN_LINK as any) || 5,
      connectTimeoutMS: 60000,
      waitQueueTimeoutMS: 60000,
      socketTimeoutMS: process.env.MONGODB_SOCKET_TIMEOUT_MS as any,
      maxIdleTimeMS: 300000,
      retryWrites: true,
      retryReads: true,
      serverSelectionTimeoutMS: (process.env.MONGODB_SERVER_SELECTION_TIMEOUT_MS as any) || 60000,
      w: 'majority'
    });
    console.log('mongo connected2');
    console.log(
      'process.env.MONGODB_SERVER_SELECTION_TIMEOUT_MS ',
      process.env.MONGODB_SERVER_SELECTION_TIMEOUT_MS
    );
    console.log('process.env.DB_MIN_LINK ', process.env.DB_MIN_LINK);
    console.log('process.env.MONGODB_SOCKET_TIMEOUT_MS ', process.env.MONGODB_SOCKET_TIMEOUT_MS);

    connectedCb?.();

    return db;
  } catch (error) {
    addLog.error('Mongo connect error', error);

    await db.disconnect();

    await delay(1000);
    return connectMongo(props);
  }
}
