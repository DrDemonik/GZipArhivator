using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.IO.Compression;
using System.Threading;


namespace TestApp1
{
    class Program
    {
        /// <summary>
        /// Метод контроля вводимых данных
        /// </summary>
        /// <param name="args">Вводимые параметры</param>
        /// <param name="filePath">Полный путь файла</param>
        /// <param name="newFilePath">Полный путь нового файла</param>
        /// <param name="comOrDecom">Флаг компрессии или декомпрессии</param>
        static void ControlArgs(string[] args, out string filePath, out string newFilePath, out bool comOrDecom)
        {
            if (args == null) throw new ArgumentNullException(" Отствуют вводимые параметры.");
            if (args.Length != 3) throw new ArgumentException(" Неверное количество введёных параметров.");
            filePath = args[1];
            newFilePath = args[2];
            //Проверка вводимых символов
            if ((args[0][0] == 'c') || (args[0][0] == 'C'))
            {
                //Выбор компрессии
                comOrDecom = true;
            }
            else
            {
                if ((args[0][0] == 'd') || (args[0][0] == 'D'))
                {
                    //Выбор декомпресии
                    comOrDecom = false;
                } else throw new ArgumentException("Выбран неверный тип выполнения, введите compress или decompress при следующей загрузки программы.",args[0]);
            }


        }


        static void Main(string[] args)
        {
            try
            {      
                //Полный путь файла
                string filePath;
                //Полный путь нового файла
                string newFilePath;
                //Флаг компрессии или декомпрессии
                bool comOrDecom;
                //Запуск метода контроля вводимых данных
                ControlArgs(args, out filePath, out newFilePath, out comOrDecom);//Контроль вводимых данных
                //Поиск файла
                if (File.Exists(filePath))
                {
                    FileInfo fileSource;
                    
                    if (comOrDecom)
                    {
                        //Инициализация обьекта информации о файле
                        fileSource = new FileInfo(filePath);
                        //Путь для новой папки
                        string dir = fileSource.DirectoryName + @"\Compress\";
                        //Инициализация объекта новой папки
                        DirectoryInfo dirForCompress = new DirectoryInfo(dir);
                        //Создание новой папки
                        dirForCompress.Create();
                        //Вычесление пути для нового файла
                        newFilePath = dirForCompress.FullName + fileSource.Name + ".gz";
                        //Запуск метода компресии
                        CompressFile(fileSource, filePath, newFilePath);
                    }
                    else
                    {
                        //Инициализация обьекта информации о файле
                        fileSource = new FileInfo(filePath);
                        //Проверка типа файла, вдруг не архив
                        if (fileSource.Extension != ".gz") throw new Exception("Неверный тип файла. Требуется тип файла .gz");
                        //Путь для новой папки
                        string dir = fileSource.DirectoryName + @"\Decompress\";
                        //Инициализация объекта новой папки
                        DirectoryInfo dirForDecompress = new DirectoryInfo(dir);
                        //Создание новой папки
                        dirForDecompress.Create();
                        //Вычисление нового имени файла
                        string name= fileSource.Name.Remove(fileSource.Name.Length - 3, 3);
                        //Вычесление пути для нового файла
                        newFilePath = dirForDecompress.FullName + name;   
                        //Запуск метода декомпресии
                        DecompressFile(fileSource, filePath, newFilePath);
                    }
                }
                else throw new DirectoryNotFoundException("Файл не найден!");
            }
            //Вывод исключейний
            catch (DirectoryNotFoundException e)
            {
                Console.WriteLine(e.Message);
            }
            catch(ArgumentException e)
            {
                Console.WriteLine(e.Message);
            }
            catch(Exception e)
            {
                Console.WriteLine(e.Message);
            }
            finally
            {
                Console.Read();
            }
        }


        /// <summary>
        /// Метод компрессии файла по частям
        /// </summary>
        /// <param name="fileSource">Файл для компресии</param>
        /// <param name="filePath">Полный путь файла</param>
        /// <param name="newFilePath">Полный путь компрессированного файла</param>
        static void CompressFile(FileInfo fileSource,string filePath, string newFilePath)
        {
            Console.WriteLine("Компрессия файла: {0} из папки {1} в папку {2}", fileSource.Name,filePath,newFilePath);
            //Обьект для синхронизации потоков
            object forLock = new object();
            //Очередь FIFO для записи
            Queue<Thread> fifoThreads = new Queue<Thread>();
            //Кол-во процов в системе
            int countProcessor = Environment.ProcessorCount;
            //семафор для количества активных потоков, 2х от колво процессоров
            Semaphore semaforForStart = new Semaphore(countProcessor*2, countProcessor*2);
            //Размер части файла
            int partSize = 1024 * 1024 * 1024;
            //Вычесляем размер части файла
            if (partSize > fileSource.Length)
            {
                //Изменяем размер части файла если размер файла  маленький
                partSize = ((int)fileSource.Length / (countProcessor+1))+8;
            }
            else
            {
                //Изменяем размер части файла для больших файлов чтобы не было преполнения адресного пространсва в обьектах MemoryStream
                partSize = (1024 * 1024 * 1024) / (3 * countProcessor * 2);
            }
            //Оставшаяся часть файла
            long sizeRemaining = fileSource.Length;

            using (FileStream originalFileStream = new FileStream(fileSource.FullName, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                using (FileStream compressedFileStream = File.Create(newFilePath))
                {
                    //Проверка файлов на доступность 
                    if ((originalFileStream.CanRead) && (compressedFileStream.CanWrite))
                    {
                        //Для чтения из файла, оределяет позицию потока
                        long posirionInStream = 0;
                        //Лист потоков
                        List<Thread> threadsList = new List<Thread>();
                        //Номер части файла
                        long i = 0;
                        //Проверка оставшейся части файла
                        while (sizeRemaining != 0)
                        {
                            //Блок потока если активных потоков больше чем 2х процессоров
                            semaforForStart.WaitOne();
                            //Определяем оставшийся размер всего файла
                            sizeRemaining = (long)(fileSource.Length - (i * (long)partSize));
                            //Получаем размер последней части считываемого файла
                            if (sizeRemaining < (long)partSize)
                            {
                                partSize = (int)sizeRemaining;
                                sizeRemaining = 0;
                            }
                            //Создаём обьект с будущими потоками
                            ThreadForComressAndSaveData t = new ThreadForComressAndSaveData(forLock, fifoThreads, semaforForStart, originalFileStream, posirionInStream, partSize, compressedFileStream, countProcessor);
                            //Создаём поток чтения и компресии
                            Thread threadRead = new Thread(t.ReadAndCompress);
                            //Создаём поток записи
                            Thread threadWrite = new Thread(t.Writer);
                            //Именнуем поток чтения и компресии
                            threadRead.Name = i.ToString();
                            //Именнуем поток записи
                            threadWrite.Name = threadRead.Name;
                            //Добавляем поток чтения и записи в очередь FIFO
                            fifoThreads.Enqueue(threadRead);
                            //Также добавляем оба потока в список потоков                            
                            threadsList.Add(threadWrite);
                            threadsList.Add(threadRead);
                            //Запускаем потоки
                            threadRead.Start();
                            threadWrite.Start();
                            //Изменяем будущую позицию следующей части файла
                            posirionInStream += (long)partSize;
                            //Изменяем номер следующей части файла
                            i++;
                        }
                        //Счётчик выполненых потоков
                        int counterThreads = 0;
                        //Проверка на окончание работы потоков
                        do
                        {
                            threadsList[counterThreads].Join();
                            counterThreads++;
                            Console.WriteLine("Поток выполнен");
                        } while (counterThreads < threadsList.Count);
                    }
                    else throw new Exception("Файл не доступен для чтения или записи!");
                }

                FileInfo info = new FileInfo(newFilePath);
                Console.WriteLine("Компрессия файла {0} из {1} байт в {2} байт.", fileSource.Name, fileSource.Length.ToString(), info.Length.ToString());
            }
        }


        /// <summary>
        /// Метод декомпрессии файла по частям
        /// </summary>
        /// <param name="fileSource">Файл для декомпресии</param>
        /// <param name="filePath">Полный путь файла</param>
        /// <param name="newFilePath">Полный путь декомпрессированного файла</param>
        static void DecompressFile(FileInfo fileSource, string filePath, string newFilePath)
        {
            Console.WriteLine("Декомпрессия файла: {0} из папки {1} в папку {2}", fileSource.Name, filePath, newFilePath);
            //Обьект для синхронизации потоков
            object forLock = new object();
            //Очередь FIFO для записи
            Queue<Thread> fifoThreads = new Queue<Thread>();
            //Кол-во процов в системе
            int countProcessor = Environment.ProcessorCount;
            //семафор для количества активных потоков
            Semaphore semaforForStart = new Semaphore(countProcessor * 2, countProcessor * 2);
            //Длина компрессионного блока
            int compressedBlockLength;
            //Буффер байтов для определения длины компрессионного блока
            byte[] buffer = new byte[8];
            using (FileStream originalFileStream = new FileStream(fileSource.FullName, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                using (FileStream decompressedFileStream = File.Create(newFilePath))
                {
                    //Проверка файлов на доступность 
                    if ((originalFileStream.CanRead) && (decompressedFileStream.CanWrite))
                    {
                        //Лист потоков
                        List<Thread> threadsList = new List<Thread>();
                        //Номер части файла
                        long i = 0;
                        //Для чтения из файла, оределяет позицию потока
                        long startPosition = 0;
                        //Семафор для чтения
                        Semaphore semaforForREAD = new Semaphore(1, 1);
                        do
                        {
                            //Блок потока если активных потоков больше чем 2х процессоров
                            semaforForStart.WaitOne();
                            //Блок потока если происходит чтение из файла
                            semaforForREAD.WaitOne();
                            //Установка позиции в потоке файла
                            originalFileStream.Position = startPosition;
                            //Чтение в буффер байтов
                            originalFileStream.Read(buffer, 0, 8);
                            //Сигнал всем заблокированным потокам о выходе из семафора
                            semaforForREAD.Release();
                            //Вычеесление длины компрессионного блока
                            compressedBlockLength = BitConverter.ToInt32(buffer, 4);
                            //Создаём объект с будущими потоками декомпрессии и записи
                            ThreadForDecomressAndSaveData t = new ThreadForDecomressAndSaveData(forLock, fifoThreads, semaforForStart, semaforForREAD, originalFileStream, compressedBlockLength, startPosition, decompressedFileStream, buffer, countProcessor);
                            //Создаём поток чтения и декомпрессии
                            Thread threadRead = new Thread(t.ReadAndDecompress);
                            //Создаём поток записи
                            Thread threadWrite = new Thread(t.Writer);
                            //Именнуем поток чтения и декомпрессии
                            threadRead.Name = i.ToString();
                            //Иеннуем поток записи
                            threadWrite.Name = threadRead.Name;
                            //Добавляем поток чтения и декомпрессии в очередь FIFO
                            fifoThreads.Enqueue(threadRead);
                            //Также добавляем потоки в список потоков
                            threadsList.Add(threadWrite);
                            threadsList.Add(threadRead);
                            //Запускаем потоки
                            threadRead.Start();
                            threadWrite.Start();
                            //Изменяем будущую стартовую позицию следующей части компрессированного файла
                            startPosition += compressedBlockLength;
                            //Инкрементируем номер следующей части файла                            
                            i++;
                        }
                        while (startPosition < originalFileStream.Length);
                        //Счётчик выполненых потоков
                        int counterThreads = 0;
                        //Проверка на окончание работы потоков
                        do
                        {
                            threadsList[counterThreads].Join();
                            //Инкрементируем счётчик выполненых потоков
                            counterThreads++;
                            Console.WriteLine("Поток выполнен");
                        } while (counterThreads < threadsList.Count);

                    }
                    else throw new Exception("Файл не доступен для чтения или записи!");
                }
                FileInfo info = new FileInfo(newFilePath);
                Console.WriteLine("Decompressed {0} from {1} to {2} bytes.",
                fileSource.Name, fileSource.Length.ToString(), info.Length.ToString());
            }

        }
    }

    
    /// <summary>
    /// Класс компресии
    /// </summary>
    class ThreadForComressAndSaveData
    {
        //Семафор для активных потоков компресии
        static Semaphore semaforForStart;
        //Семафор для чтения
        static Semaphore semaforForREAD=new Semaphore(1,1);
        //Очередь FIFO
        Queue<Thread> fifoThreads;
        //Обьект для синхронизации потоков
        object forLock;
        //Счётчик писателей
        static int writing;
        //Счётчик читателей
        static int reading;
        //Файловый поток чтения рабочего файла
        private FileStream originalFileStream;
        //Позиция для файлового потока чтения
        private long posirionInStream;
        //Размер части файла
        private int partSize;
        //Файловый поток записи компрессируемого файла
        private FileStream compressedFileStream;
        //Буффер байтов части файла
        private byte[] bufferData;
        //Буффер байтов компрессированной части файла
        private byte[] compressedBufferData;
        //Флаг возможности записи
        private bool canWriting;

        private int countProcessor;


        //Конструктор класса
        public ThreadForComressAndSaveData( object _forLock, Queue<Thread> _fifoThreads,Semaphore _semaforForStart, FileStream _originalFileStream,long _posirionInStream, int _partSize, FileStream _compressedFileStream, int _countProcessor) //Конструктор получает имя функции и номер до кторого ведется счет
        {
            forLock = _forLock;
            semaforForStart = _semaforForStart;
            fifoThreads = _fifoThreads;
            originalFileStream = _originalFileStream;
            posirionInStream = _posirionInStream;
            partSize = _partSize;
            compressedFileStream = _compressedFileStream;
            canWriting = false;
            countProcessor = _countProcessor;


        }

        //Метод потока чтения и компресии
        public void ReadAndCompress()
        {
            Console.WriteLine("Поток: {0} Старт чтения и компресии!", Thread.CurrentThread.Name);
            lock (forLock)
            {
                while (reading > countProcessor)
                {
                    Console.WriteLine("Поток: {0} Ждёт чтение и компрессию", Thread.CurrentThread.Name);
                    //Блокировка потока
                    Monitor.Wait(forLock);
                }
                //Инкрементация читателей
                reading++;
            }

            #region Блок чтения
            //Блок потока если происходит чтение из файла
            semaforForREAD.WaitOne();
            Console.WriteLine("Поток: {0} Чтение и компрессия разрешена", Thread.CurrentThread.Name);
            //Инициализация буфера 
            bufferData = new byte[partSize];
            //Установка позиции чтения
            originalFileStream.Position = posirionInStream;
            //Чтение из файла в буфер
            int countByteRead = originalFileStream.Read(bufferData, 0, bufferData.Length);
            Console.WriteLine("Поток: {0} Чтение с {1} позиции по {2} позицию, требуется прочесть {3} байт, прочитано {4} байт", Thread.CurrentThread.Name, posirionInStream, originalFileStream.Position, partSize, countByteRead);
            //Исключение
            if (countByteRead != bufferData.Length) throw new ArgumentException("Считано неверное количество байт!");
            //Установка позиции чтения для следующего потока
            originalFileStream.Position = posirionInStream + partSize;
            //Сигнал всем заблокированным потокам о выходе из семафора
            semaforForREAD.Release();
            #endregion

            #region Блок компрессии
            using (MemoryStream ms = new MemoryStream())
            {
                using (GZipStream compressionStream = new GZipStream(ms, CompressionMode.Compress, true))
                {
                    Console.WriteLine("Поток: {0} Компрессия с позиции {1}!", Thread.CurrentThread.Name, posirionInStream);
                    //Компрессия
                    compressionStream.Write(bufferData, 0, bufferData.Length); ;

                }
                //Запись из оперативы в буфер сжатых данных
                compressedBufferData = ms.ToArray();
                Console.WriteLine("Поток: {0} Компрессия закончена! Размер сжатого блока {1} из {2}", Thread.CurrentThread.Name, compressedBufferData.Length, countByteRead);
                //Запись в байтовом виде, длины сжатых данных, после магического числа GZip
                BitConverter.GetBytes(compressedBufferData.Length).CopyTo(compressedBufferData, 4);
            }
            #endregion

            Console.WriteLine("Поток: {0} Финиш чтения и компресии!", Thread.CurrentThread.Name);
            lock (forLock)
            {
                //Декрементация читателей
                reading--;
                //Разрешение записии
                canWriting = true;
                //Разблокировка ждущих потоков
                Monitor.PulseAll(forLock);
                
            }
        }

        //Метод потока записи
        public void Writer()
        {
            lock (forLock)
            {
                
                Console.WriteLine("Поток: {0} Попытка записи", Thread.CurrentThread.Name);
                while (((Thread.CurrentThread.Name != fifoThreads.Peek().Name) || (!canWriting))|| (writing > 0)|| (compressedBufferData == null))
                {
                    Console.WriteLine("Поток: {0} 8.Запись ждёт", Thread.CurrentThread.Name);
                    //Блокировка потока
                    Monitor.Wait(forLock);
                }
                //Инкрементация писателей
                writing++;
            }
            Console.WriteLine("Поток: {0} 9.Запись в файл!", Thread.CurrentThread.Name);
            //Запись компрессированных(сжатых) данных в файл
            compressedFileStream.Write(compressedBufferData, 0, compressedBufferData.Length);
            Console.WriteLine("Поток: {0} 10.Запись Завершена!", Thread.CurrentThread.Name);
            lock (forLock)
            {
                //Удаление потока чтения и компресии из списка FIFO
                fifoThreads.Dequeue();
                //Декрементация писателей
                writing--;
                //Разблокировка ждущий потоков
                Monitor.PulseAll(forLock);
                //Разблокировка семафора
                semaforForStart.Release();
            }
        }
    }

    class ThreadForDecomressAndSaveData//Класс потока компресии
    {
        //Семафор для активных потоков компресии
        static Semaphore semaforForStart;
        //Семафор для чтения
        static Semaphore semaforForREAD;
        //Очередь потоков FIFO 
        Queue<Thread> fifoThreads;
        //Обьект для синхронизации потоков
        object forLock;
        //Счётчик писателей
        static int writing;
        //Счётчик читателей
        static int reading;
        //Файловый поток чтения рабочего файла
        private FileStream originalFileStream;
        //Позиция начала чтения
        private long startPosition;
        //Длина компрессированного блока
        private int compressedBlockLength;
        //Файловый поток записи декомпрессированного файла
        private FileStream decompressedFileStream;
        //Буфер для сжатых данных
        private byte[] buffer;
        //Буфер для декомпрессированных данных
        private byte[] decompressedBufferData;
        //Флаг разрешения записи
        private bool canWriting;
        //Количество процесоров 
        private int countProcessor;


        //Конструктор класса
        public ThreadForDecomressAndSaveData( object _forLock, Queue<Thread> _fifoThreads,Semaphore _semaforForStart, Semaphore _semaforForREAD, FileStream _originalFileStream, int _compressedBlockLength, long _startPosition, FileStream _decompressedFileStream,byte[] _buffer, int _countProcessor) //Конструктор получает имя функции и номер до кторого ведется счет
        {
            forLock = _forLock;
            semaforForStart = _semaforForStart;
            fifoThreads = _fifoThreads;
            originalFileStream = _originalFileStream;
            startPosition = _startPosition;
            compressedBlockLength = _compressedBlockLength;
            decompressedFileStream = _decompressedFileStream;
            canWriting = false;
            buffer = _buffer;
            semaforForREAD = _semaforForREAD;
            countProcessor = _countProcessor;
        }

        
        //Метод потока чтения и декомпресии
        public void ReadAndDecompress()
        {
            Console.WriteLine("Поток: {0} СТАРТ чтения и декомпрессии!", Thread.CurrentThread.Name);
            lock (forLock)
            {
                while (reading > countProcessor)
                {
                    Console.WriteLine("Поток: {0} Ждёт чтение и декомпрессию", Thread.CurrentThread.Name);
                    //Блокировка потока
                    Monitor.Wait(forLock);
                }
                //Инкрементация читателей
                reading++;
                
            }
            Console.WriteLine("Поток: {0} Чтение и декомпрессия разрешена!", Thread.CurrentThread.Name);
            #region Блок чтения
            // Блок потока если происходит чтение из файла
            semaforForREAD.WaitOne();
            //Инициализация буфера сжатых данных
            byte[] compressedDataArray = new byte[compressedBlockLength + 1];
            buffer.CopyTo(compressedDataArray, 0);
            //Установка позиции чтения
            originalFileStream.Position = startPosition;
            //Чтение из файла в буфер
            int countByteRead = originalFileStream.Read(compressedDataArray, 0, compressedBlockLength);
            Console.WriteLine("Поток: {0} Чтение с {1} позиции по {2} позицию, требуется прочесть {3} байт, прочитано {4} байт", Thread.CurrentThread.Name, startPosition, originalFileStream.Position, compressedBlockLength, countByteRead);
            //Исключение
            if ((countByteRead + 1) != (compressedDataArray.Length)) throw new ArgumentException(nameof(countByteRead));
            //Определение длины части оригинального файла
            int _dataPortionSize = BitConverter.ToInt32(compressedDataArray, compressedBlockLength - 4);
            //Инициализация декомпрессированного файла
            decompressedBufferData = new byte[_dataPortionSize];
            //Сигнал всем заблокированным потокам о выходе из семафора
            semaforForREAD.Release();
            #endregion

            #region Блок декомпрессии
            using (MemoryStream ms = new MemoryStream(compressedDataArray))
            {
                using (GZipStream Decompress = new GZipStream(ms, CompressionMode.Decompress))
                {
                    Console.WriteLine("Поток: {0} Декомпрессия!", Thread.CurrentThread.Name);
                    //Декомпрессия
                    Decompress.Read(decompressedBufferData, 0, decompressedBufferData.Length);
                    Console.WriteLine("Поток: {0} Декомпресия Закончена!", Thread.CurrentThread.Name);
                }
            }

            #endregion

            Console.WriteLine("Поток: {0} ФИНИШ чтения и декомпресии!", Thread.CurrentThread.Name);
            lock (forLock)
            {
                //Декрементация читателей
                reading--;
                //Разблокировка ждущих потоков
                Monitor.PulseAll(forLock);
                //Разрешение записи
                canWriting = true;
                
            }
        }

        //Метод потока записи
        public void Writer()
        {
            lock (forLock)
            {
                
                Console.WriteLine("Поток: {0} З. Попытка записи", Thread.CurrentThread.Name);
                while (((Thread.CurrentThread.Name != fifoThreads.Peek().Name) || (!canWriting))|| (writing > 0)|| (decompressedBufferData == null))
                {
                    Console.WriteLine("Поток: {0} З. Запись ждёт", Thread.CurrentThread.Name);
                    //Блокировка потока
                    Monitor.Wait(forLock);
                }
                //Интрементация писателей
                writing++;
            }

            Console.WriteLine("Поток: {0} З. Запись в файл!", Thread.CurrentThread.Name);
            //Запись в файл декомпрессированных данных
            decompressedFileStream.Write(decompressedBufferData, 0, decompressedBufferData.Length);
            Console.WriteLine("Поток: {0} З. Запись Завершена!", Thread.CurrentThread.Name);

            lock (forLock)
            {
                //Удаление потока из очереди FIFO
                fifoThreads.Dequeue();
                //Декреминтация писателей
                writing--;
                //Раблокировка ждущих потоков
                Monitor.PulseAll(forLock);
                //Сигнал всем заблокированным потокам о выходе из семафораы
                semaforForStart.Release();
            }
        }
    }
}

