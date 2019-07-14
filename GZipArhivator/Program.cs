using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.IO.Compression;
using System.Threading;


namespace GZipArhivator
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
                            Comress t = new Comress(forLock, fifoThreads, semaforForStart, originalFileStream, posirionInStream, partSize, compressedFileStream, countProcessor);
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
                            Decomress t = new Decomress(forLock, fifoThreads, semaforForStart, semaforForREAD, originalFileStream, compressedBlockLength, startPosition, decompressedFileStream, buffer, countProcessor);
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
}

