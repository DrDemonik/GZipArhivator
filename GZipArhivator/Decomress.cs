using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;

namespace GZipArhivator
{
    class Decomress//Класс потока компресии
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
        public Decomress(object _forLock, Queue<Thread> _fifoThreads, Semaphore _semaforForStart, Semaphore _semaforForREAD, FileStream _originalFileStream, int _compressedBlockLength, long _startPosition, FileStream _decompressedFileStream, byte[] _buffer, int _countProcessor) //Конструктор получает имя функции и номер до кторого ведется счет
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
                while (((Thread.CurrentThread.Name != fifoThreads.Peek().Name) || (!canWriting)) || (writing > 0) || (decompressedBufferData == null))
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
