package badger_sdk

import (
	"fmt"
	"time"
)

type LogLevel string

const (
	LogDebug   LogLevel = "DEBUG"
	LogInfo    LogLevel = "INFO"
	LogWarning LogLevel = "WARNING"
	LogError   LogLevel = "ERROR"
)

func GetLevelByName(name string) LogLevel {
	switch name {
	case "DEBUG":
		return LogDebug
	case "INFO":
		return LogInfo
	case "WARNING":
		return LogWarning
	case "ERROR":
		return LogError
	default:
		return LogInfo
	}
}

type BadgerDBMaster struct {
	Dir         string
	ValueDir    string
	SyncWrites  bool
	Compression string

	InMemory             bool
	RamLimitMemory       int64
	ReadOnly             bool
	WithMetrics          bool
	GCInterval           time.Duration
	NumGoroutines        int
	ValueThreshold       int64
	ValueLogFileSize     int64
	BaseTableSize        int64
	NumCompactors        int
	ZstdCompressionLevel int
	DetectConflicts      bool
	Encoder              string
	NumVersionsToKeep    int
}

type Options struct {
	// Dir — каталог для LSM-части (SST, MANIFEST). Должен существовать или будет создан.
	// При InMemory=true игнорируется.
	Dir string

	// ValueDir — каталог для value log (крупные значения). Может совпадать с Dir.
	// При InMemory=true игнорируется.
	ValueDir string

	// InMemory — полностью в памяти, без файлов на диске. Полезно для тестов/кэшей.
	// При включении Dir/ValueDir игнорируются, данные не переживают перезапуск.
	InMemory bool

	// ReadOnly — открыть БД только для чтения. Записи/компакции/GC невозможны.
	// Требует, чтобы файлы БД уже существовали.
	ReadOnly bool

	// WithMetrics — включить внутренние метрики (hits/misses и пр.) для кешей и т.д.
	// Нужен bo = bo.WithMetricsEnabled(true), иначе счётчики будут нулевыми.
	WithMetrics bool

	// GCInterval — периодический запуск value-log GC (если вы это делаете сами через s.runGC).
	// Badger сам GC «по таймеру» не запускает; эту периодику задаёте вы.
	GCInterval time.Duration

	// SyncWrites — fsync на каждую запись (жертвуем скоростью ради максимальной надёжности).
	// false обычно быстрее, но возможна потеря последних записей при сбое питания/процесса.
	SyncWrites bool

	// NumGoroutines — степень параллелизма фоновых операций Badger (компакции, чтение/запись).
	// Не путать с NumCompactors: это общий «пул» воркеров для разных задач.
	NumGoroutines int

	// LoggingLevel — уровень логирования Badger (DEBUG/INFO/WARNING/ERROR).
	LoggingLevel LogLevel

	// ------------------- ПАМЯТЬ / КЕШИ / BUFFERS -------------------

	// BlockCacheSize — размер кеша «блоков» SST (Ristretto с TinyLFU/SLRU-эвикцией).
	// Держит в памяти часто читаемые блоки таблиц, снижая обращения к диску и повторную декомпрессию.
	// Лимит в байтах; при превышении — эвикция «холодных» элементов.
	BlockCacheSize int64

	// IndexCacheSize — размер кеша индексов SST и Bloom-фильтров (также Ristretto).
	// Ускоряет ответы «ключ точно отсутствует/в каком файле искать» без I/O.
	// Лимит в байтах; при превышении — эвикция.
	IndexCacheSize int64

	// MemTableSize — размер ОДНОЙ memtable (байты). При достижении лимита активная memtable
	// становится immutable и асинхронно сбрасывается в SST. Новая активная memtable создаётся отдельно.
	// Большие memtable улучшают последовательность записи и уменьшают write amplification,
	// но увеличивают пиковое RAM-потребление и задержку флаша.
	MemTableSize int64

	// NumMemtables — общее число memtable-слотов: 1 активная + (NumMemtables-1) immutable в очереди на сброс.
	// Если все слоты заняты (активная заполнена, а immutable ещё не сброшены), запись будет притормаживаться
	// (backpressure) до завершения флаша. Итоговое пиковое RAM-потребление под memtable
	// ≈ NumMemtables * MemTableSize.
	NumMemtables int

	// ------------------- ПРОЧИЕ ПАРАМЕТРЫ ХРАНЕНИЯ -------------------

	// ValueThreshold — порог (байты), выше которого значение кладётся в value log,
	// а в LSM (SST) хранится только указатель. Меньшие значения инлайнатся в LSM.
	// Выше порог — меньше LSM, больше vlog I/O. Ниже порог — больше LSM, меньше vlog.
	ValueThreshold int64

	// ValueLogFileSize — максимальный размер одиночного файла value log (байты), после чего
	// Badger начинает новый vlog-файл. Влияет на частоту GC и количество открытых файлов.
	ValueLogFileSize int64

	// BaseTableSize — целевой базовый размер SST-таблицы (байты). Фактические размеры таблиц
	// на уровнях растут кратно базовому (согласно внутренним коэффициентам), влияя на стратегию компакций.
	BaseTableSize int64

	// NumCompactors — число воркеров компакции. Больше — быстрее переработка уровней при высокой нагрузке,
	// но выше конкуренция за I/O и память.
	NumCompactors int

	// Compression — алгоритм сжатия блоков SST. "" (по умолчанию) — ZSTD; "none" — без сжатия.
	// "" | "none" | "zstd" | "snappy" | "lz4" | "zlib" —
	Compression string

	// ZSTDCompressionLevel — уровень ZSTD (0 — по умолчанию; <0 — быстрее/хуже; >0 — медленнее/лучше).
	// Влияет на место на диске и CPU-времена (чтение/запись/компакции).
	ZSTDCompressionLevel int

	// DetectConflicts — детект конфликтов транзакций (write-write). true — безопаснее, но дороже.
	// Можно отключать при полном внешнем контроле параллелизма/последовательности.
	DetectConflicts bool

	// EncryptionKey — ключ шифрования (AES-CTR): 16/24/32 байта. Пустой срез — без шифрования.
	// Применяется к данным на диске (SST/vlog).
	EncryptionKey []byte

	// NumVersionsToKeep — число версий ключа, которые будут храниться в БД.
	// При записи Badger будет хранить только последние N версий ключа.
	NumVersionsToKeep int

	// Encoder - маршалер для сериализации/десериализации объектов
	Encoder Encoder
}

// MemoryLimit ComputeMemoryLimit вычисляет разумные значения для кешей и memtables по переданнуму лимиту памяти
type MemoryLimit struct {
	BlockCacheSize int64 // например, 8 * 1024 * 1024 * 1024 (8 GiB)
	IndexCacheSize int64 // например, 4 * 1024 * 1024 * 1024 (4 GiB)
	MemTableSize   int64 // размер одной memtable
	NumMemtables   int   // максимум 2
}

const (
	B   int64 = 1
	KiB int64 = 1024 * B
	MiB int64 = 1024 * KiB
	GiB int64 = 1024 * MiB
)

type LoadProfile struct {
	SafetyFraction    float64 // доля от total, напр. 0.10
	SafetyMin         int64   // минимум, напр. 128 MiB
	SafetyMaxFraction float64 // максимум от total, напр. 0.25

	// Fallback-доли под memtable, когда нет write-rate: small (<6 GiB), medium (6–12 GiB), large (>=12 GiB)
	MemtableRatios [3]float64

	// --- Кэширование ---
	// Разделение кешей: block:index = Num:Den (если не переопределено в hints)
	BlockToIndexRatioNum int64
	BlockToIndexRatioDen int64

	// Минимальные «полы» кешей (на диск)
	MinBlockCache int64
	MinIndexCache int64

	// Минимальная доля бюджета, которая должна остаться на кеши (после memtable), 0..1
	MinCacheFraction float64
}

// Профили нагрузки
const (
	ReadLoad      = "read"
	WriteLoad     = "write"
	ReadWriteLoad = "read-write"
)

var loadMap = map[string]LoadProfile{
	ReadLoad: {
		SafetyFraction:       0.10,
		SafetyMin:            128 * MiB,
		SafetyMaxFraction:    0.25,
		MemtableRatios:       [3]float64{0.08, 0.10, 0.125},
		BlockToIndexRatioNum: 3, // 3:1 в пользу block
		BlockToIndexRatioDen: 4,
		MinBlockCache:        512 * MiB,
		MinIndexCache:        256 * MiB,
		MinCacheFraction:     0.70, // читающей нагрузке нужен большой кеш
	},
	WriteLoad: {
		SafetyFraction:       0.10,
		SafetyMin:            128 * MiB,
		SafetyMaxFraction:    0.25,
		MemtableRatios:       [3]float64{0.15, 0.20, 0.25},
		BlockToIndexRatioNum: 1, // 1:1
		BlockToIndexRatioDen: 2,
		MinBlockCache:        256 * MiB,
		MinIndexCache:        128 * MiB,
		MinCacheFraction:     0.40,
	},
	ReadWriteLoad: {
		SafetyFraction:       0.10,
		SafetyMin:            128 * MiB,
		SafetyMaxFraction:    0.25,
		MemtableRatios:       [3]float64{0.125, 0.15, 0.20},
		BlockToIndexRatioNum: 2, // 2:1
		BlockToIndexRatioDen: 3,
		MinBlockCache:        256 * MiB,
		MinIndexCache:        128 * MiB,
		MinCacheFraction:     0.60,
	},
}

func ComputeMemoryPreset(memoryLimit int64, profile string) *MemoryLimit {
	return ComputeMemoryPresetWithClamp(memoryLimit, 1, 128, profile, 3)
}

// ComputeMemoryPresetWithClamp Основная функция
func ComputeMemoryPresetWithClamp(
	memoryLimitBytes int64,
	clampMinGiB int64,
	clampMaxGiB int64,
	profileName string,
	wantMemtables int, // желаемое число memtables
) *MemoryLimit {
	const (
		ALIGN           = 64 * MiB
		maxMemtableSize = 2 * GiB
		minMemtableSize = 64 * MiB
	)

	if clampMinGiB < 1 {
		clampMinGiB = 1
	}
	if clampMaxGiB > 128 {
		clampMaxGiB = 128
	}
	if clampMinGiB > clampMaxGiB {
		clampMinGiB, clampMaxGiB = clampMaxGiB, clampMinGiB
	}

	profileConfig, exists := loadMap[profileName]
	if !exists {
		profileConfig = loadMap[ReadWriteLoad]
	}

	minLimitBytes := clampMinGiB * GiB
	maxLimitBytes := clampMaxGiB * GiB

	// Зажимаем общий лимит
	totalLimitBytes := memoryLimitBytes
	if totalLimitBytes < minLimitBytes {
		totalLimitBytes = minLimitBytes
	}
	if totalLimitBytes > maxLimitBytes {
		totalLimitBytes = maxLimitBytes
	}

	// Резерв под рантайм
	safetyReserveBytes := int64(float64(totalLimitBytes) * profileConfig.SafetyFraction)
	if safetyReserveBytes < profileConfig.SafetyMin {
		safetyReserveBytes = profileConfig.SafetyMin
	}
	if maximum := int64(float64(totalLimitBytes) * profileConfig.SafetyMaxFraction); safetyReserveBytes > maximum {
		safetyReserveBytes = maximum
	}

	workingBudgetBytes := totalLimitBytes - safetyReserveBytes
	if workingBudgetBytes < 0 {
		workingBudgetBytes = totalLimitBytes
	}

	// Доля под memtables по размеру системы (fallback, т.к. write-rate не знаем)
	var memtableRatio float64
	switch {
	case totalLimitBytes >= 12*GiB:
		memtableRatio = profileConfig.MemtableRatios[2]
	case totalLimitBytes >= 6*GiB:
		memtableRatio = profileConfig.MemtableRatios[1]
	default:
		memtableRatio = profileConfig.MemtableRatios[0]
	}
	if memtableRatio < 0 {
		memtableRatio = 0
	}
	if memtableRatio > 0.5 {
		memtableRatio = 0.5
	}

	memtableBudgetBytes := int64(float64(workingBudgetBytes) * memtableRatio)
	if memtableBudgetBytes < minMemtableSize {
		memtableBudgetBytes = minMemtableSize
	}

	// Начальное число memtables (авто) и верхняя граница по бюджету
	maxMemtablesByBudget := int(workingBudgetBytes / minMemtableSize)
	if maxMemtablesByBudget < 1 {
		maxMemtablesByBudget = 1
	}

	autoMemtables := 2
	if autoMemtables > maxMemtablesByBudget {
		autoMemtables = maxMemtablesByBudget
	}
	if memtableBudgetBytes < 2*minMemtableSize {
		autoMemtables = 1
	}

	memtableCount := autoMemtables
	if wantMemtables > 0 {
		memtableCount = wantMemtables
		if memtableCount > maxMemtablesByBudget {
			memtableCount = maxMemtablesByBudget
		}
		if memtableCount < 1 {
			panic("ComputeMemoryPresetWithClamp: requested memtables < 1")
		}
	}

	alignDownTo := func(value int64) int64 { return (value / ALIGN) * ALIGN }

	// Ищем первый подходящий memtableCount, уменьшая при нехватке бюджета/кэш-доли
	var (
		bytesPerMemtable    int64
		totalMemtablesBytes int64
		cacheBudgetBytes    int64
	)

	minCacheBudgetBytes := int64(float64(workingBudgetBytes) * profileConfig.MinCacheFraction)
	if minCacheBudgetBytes < 0 {
		minCacheBudgetBytes = 0
	}

	for {
		if memtableCount <= 0 {
			panic("ComputeMemoryPresetWithClamp: not enough memory for even one memtable")
		}

		// bytesPerMemtable из memtableBudgetBytes / memtableCount
		bytesPerMemtable = memtableBudgetBytes / int64(memtableCount)
		if bytesPerMemtable < minMemtableSize {
			bytesPerMemtable = minMemtableSize
		}
		if bytesPerMemtable > maxMemtableSize {
			bytesPerMemtable = maxMemtableSize
		}
		bytesPerMemtable = alignDownTo(bytesPerMemtable)
		if bytesPerMemtable <= 0 {
			bytesPerMemtable = minMemtableSize
		}

		totalMemtablesBytes = bytesPerMemtable * int64(memtableCount)
		if totalMemtablesBytes > workingBudgetBytes {
			// не влезаем — уменьшаем число memtables
			memtableCount--
			continue
		}

		cacheBudgetBytes = workingBudgetBytes - totalMemtablesBytes
		// Гарантируем минимальную долю кэшей
		if cacheBudgetBytes < minCacheBudgetBytes {
			if memtableCount == 1 {
				// Уже минимально — выходим, примем меньшую долю кэша
				break
			}
			memtableCount--
			continue
		}

		break
	}

	// Делим кеши по профилю
	blockToIndexRatioNumerator := profileConfig.BlockToIndexRatioNum
	blockToIndexRatioDenominator := profileConfig.BlockToIndexRatioDen
	if blockToIndexRatioNumerator <= 0 || blockToIndexRatioDenominator <= 0 ||
		blockToIndexRatioNumerator > blockToIndexRatioDenominator {
		blockToIndexRatioNumerator, blockToIndexRatioDenominator = 2, 3 // fallback 2:1
	}

	blockCacheBytes := (cacheBudgetBytes * blockToIndexRatioNumerator) / blockToIndexRatioDenominator
	indexCacheBytes := cacheBudgetBytes - blockCacheBytes

	// Минимумы кешей
	minBlockCacheBytes := profileConfig.MinBlockCache
	minIndexCacheBytes := profileConfig.MinIndexCache
	if blockCacheBytes < minBlockCacheBytes {
		blockCacheBytes = minBlockCacheBytes
	}
	if indexCacheBytes < minIndexCacheBytes {
		indexCacheBytes = minIndexCacheBytes
	}

	// Если не влезаем — ужмём пропорционально
	totalCacheBytes := blockCacheBytes + indexCacheBytes
	if totalCacheBytes > cacheBudgetBytes && totalCacheBytes > 0 {
		blockCacheBytes = (blockCacheBytes * cacheBudgetBytes) / totalCacheBytes
		indexCacheBytes = cacheBudgetBytes - blockCacheBytes
	}

	// Выравнивание
	blockCacheBytes = alignDownTo(blockCacheBytes)
	indexCacheBytes = alignDownTo(indexCacheBytes)
	bytesPerMemtable = alignDownTo(bytesPerMemtable)

	// Вернём остаток после выравнивания в block-cache
	leftoverBytes := cacheBudgetBytes - (blockCacheBytes + indexCacheBytes)
	if leftoverBytes > 0 {
		blockCacheBytes += leftoverBytes
	}

	if blockCacheBytes < 0 {
		blockCacheBytes = 0
	}
	if indexCacheBytes < 0 {
		indexCacheBytes = 0
	}
	if bytesPerMemtable < minMemtableSize {
		bytesPerMemtable = minMemtableSize
	}

	limits := &MemoryLimit{
		BlockCacheSize: blockCacheBytes,
		IndexCacheSize: indexCacheBytes,
		MemTableSize:   bytesPerMemtable,
		NumMemtables:   memtableCount,
	}

	// Логи
	requestedGiB := float64(memoryLimitBytes) / float64(GiB)
	clampedGiB := float64(totalLimitBytes) / float64(GiB)
	fmt.Printf("[mem-preset] profile=%s | requested=%.2f GiB -> clamped=%.2f GiB | safety=%d MiB | budget=%d MiB\n",
		profileName, requestedGiB, clampedGiB, safetyReserveBytes/MiB, workingBudgetBytes/MiB)
	fmt.Printf("[mem-preset] caches: block=%d MiB, index=%d MiB, total=%d MiB\n",
		blockCacheBytes/MiB, indexCacheBytes/MiB, (blockCacheBytes+indexCacheBytes)/MiB)
	fmt.Printf("[mem-preset] memtables: count=%d, per_table=%d MiB, total=%d MiB\n",
		memtableCount, bytesPerMemtable/MiB, (bytesPerMemtable*int64(memtableCount))/MiB)

	return limits
}
