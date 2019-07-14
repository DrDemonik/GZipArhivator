using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;

namespace GZipArhivator
{
    /// <summary>
    /// Класс компресии
    /// </summary>
    class Comress
    {
        //Семафор для активных потоков компресии
        static Semaphore semaforForStart;
        //Семафор для чтения
        static Semaphore semaforForREAD = new Semaphore(1, 1);
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
        public Comress(object _forLock, Queue<Thread> _fifoThreads, Semaphore _semaforForStart, FileStream _originalFileStream, long _posirionInStream, int _partSize, FileStream _compressedFileStream, int _countProcessor) //Конструктор получает имя функции и номер до кторого ведется счет
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
                while (((Thread.CurrentThread.Name != fifoThreads.Peek().Name) || (!canWriting)) || (writing > 0) || (compressedBufferData == null))
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
}
