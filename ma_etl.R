# =============================================================================
# MA 票务数据 ETL 流水线
# 功能：将多渠道票务数据抽取、清洗、转换后合并为统一宽表
# 输出字段：剧目、项目、剧场、场次、下单时间、渠道类型、渠道明细、
#           票价、实收、张数、收货人姓名、收货人手机、地址
# =============================================================================

library(tidyverse)
library(readxl)
library(lubridate)
library(openxlsx)

# =============================================================================
# 0. 全局配置
# =============================================================================

BASE_DIR   <- "D:/"
DATA_DIR   <- file.path(BASE_DIR, "file1")
OUTPUT_DIR <- file.path(BASE_DIR, "file2")
CACHE_DIR  <- file.path(DATA_DIR, "data_cache")

# 目标字段顺序（所有中间表最终 select 至此）
STANDARD_COLS <- c(
  "剧目", "项目", "剧场", "场次", "下单时间",
  "渠道类型", "渠道明细",
  "票价", "实收", "张数",
  "收货人姓名", "收货人手机", "地址",
  "动作日期", "动作类型"
)

# 补全缺失列为 NA，保证 bind_rows 不出意外
pad_cols <- function(df, cols = STANDARD_COLS) {
  for (col in setdiff(cols, colnames(df))) df[[col]] <- NA_character_
  df[, intersect(cols, colnames(df))]
}

# =============================================================================
# 1. 通用工具函数
# =============================================================================

# 去除数字中的千位逗号
strip_commas <- function(x) as.numeric(str_replace_all(x, ",", ""))

# 把 "MM.DD" 格式字符串解析为日期（跨年处理：月份>3 视为上一年）
parse_dot_date <- function(x, ref_year = NULL) {
  m <- str_match(x, "(\\d+)\\.(\\d+)")
  month <- as.integer(m[, 2])
  day   <- as.integer(m[, 3])
  year  <- if (!is.null(ref_year)) ref_year
           else if_else(month > 3, 2023L, 2024L)
  as.POSIXct(sprintf("%d-%02d-%02d 00:00:00", year, month, day), tz = "UTC")
}

# 把场次字符串统一为 "YYYY-MM-DD HH:MM:SS"
normalize_datetime <- function(x) {
  # 已是标准格式则直接返回
  if (all(str_detect(x, "^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$"), na.rm = TRUE))
    return(x)
  as.character(as.POSIXct(x, tz = "UTC"))
}

# 读取某目录下所有 xlsx/xls 文件并纵向合并（列名相同时）
read_dir <- function(dir, ...) {
  files <- list.files(dir, full.names = TRUE, pattern = "\\.(xlsx|xls)$")
  map_dfr(files, ~ readxl::read_xlsx(.x, ...) |> mutate(.src = .x))
}

# =============================================================================
# 2. 渠道映射表（集中维护，替换原来散落在各处的 is.element 列表）
# =============================================================================

# 2-1 票档映射：票品名称/规格 -> 面值票价
PRICE_MAP <- list(
  `180`  = c("180元"),
  `280`  = c("280元", "【周末】280元票价区", "280*1", "280"),
  `380`  = c("380元"),
  `480`  = c("480元", "480"),
  `580`  = c("580元"),
  `680`  = c("680元", "【周末】680元票价区", "680", "680*2", "680*4", "680*10",
             "单人票646元/张（原价：680元/张）", "￥646 (680*0.95)"),
  `780`  = c("780元", "780", "【早鸟周中】780元票价区", "【周中】780元票价区",
             "780*2", "780*2（套票）"),
  `880`  = c("880元", "880", "【早鸟周末】880元票价区", "【周末】880元票价区",
             "单人票880元/张", "单人票836元/张（原价：880元/张）",
             "880*1", "880*2", "880*3（套票）",
             "【9.5折】面值880元单人票", "三人套票（880*3）",
             "【9.3折】面值880元*3张套票"),
  `980`  = c("980元", "980", "980*2"),
  `1080` = c("1080元", "1080", "单人票1080元", "单人票1080元/张",
             "单人票:1026元（原价1080元/张）", "1080*1", "1080*2", "1080*3",
             "1080*3（套票）", "三人套票（1080*3）",
             "【9.5折】面值1080元单人票", "【9.3折】面值1080元*3张套票"),
  `1180` = c("1180元", "1180", "VIP(1180.00)"),
  `1280` = c("1280元", "1280", "单人票1280元/张", "单人票：1280元/张(送场刊）",
             "【早鸟周末】1280元票价区（送场刊）", "票种：VIP1280",
             "VIP(1280.00)")
)

lookup_price <- function(x) {
  result <- rep(NA_real_, length(x))
  for (price_str in names(PRICE_MAP)) {
    result[x %in% PRICE_MAP[[price_str]]] <- as.numeric(price_str)
  }
  unknown <- is.na(result) & !is.na(x)
  if (any(unknown)) {
    warning("未知票档：", paste(unique(x[unknown]), collapse = " | "))
  }
  result
}

# 2-2 套票张数映射（规格 -> 张数倍数）
QTY_MAP_1 <- c(
  "单人票880元/张", "单人票1080元/张", "单人票1280元/张",
  "单人票：1280元/张(送场刊）", "单人票:1026元（原价1080元/张）",
  "单人票836元/张（原价：880元/张）", "单人票:1280元",
  "单人票646元/张（原价：680元/张）", "单人票266元/张（原价：280元/张）",
  "280*1", "1080*1", "880*1", "680", "1080", "880", "780", "980",
  "280", "480", "1180", "1280"
)
QTY_MAP_2 <- c(
  "780*2（套票）", "280*2", "880*2", "680*2", "1080*2", "980*2",
  "双人套票（980*2）"
)
QTY_MAP_3 <- c(
  "1080*3（套票）", "280*3", "1080*3", "880*3（套票）",
  "3人套票：2700元/套（原价：1080*3）",
  "3人套票：2420元/套（原价：880*3）",
  "三人套票（1080*3）", "三人套票（880*3）",
  "【9.3折】面值1080元*3张套票", "【9.3折】面值880元*3张套票"
)
QTY_MAP_4  <- c("280*4", "680*4")
QTY_MAP_10 <- c("680*10")

lookup_qty_multiplier <- function(x) {
  result <- rep(NA_integer_, length(x))
  result[x %in% QTY_MAP_1]  <- 1L
  result[x %in% QTY_MAP_2]  <- 2L
  result[x %in% QTY_MAP_3]  <- 3L
  result[x %in% QTY_MAP_4]  <- 4L
  result[x %in% QTY_MAP_10] <- 10L
  unknown <- is.na(result) & !is.na(x)
  if (any(unknown)) {
    warning("未知数量规格：", paste(unique(x[unknown]), collapse = " | "))
  }
  result
}

# 2-3 深圳滨海渠道类型映射
SZBH_CHANNEL_MAP <- list(
  猫眼 = c("猫眼（T3渠道）"),
  剧场 = c("深圳滨海电商", "市场部票务", "柜台销售"),
  大麦 = c("大麦（T3渠道）")
)

# 2-4 场次字符串标准化字典（各分销渠道特有格式 -> 标准 datetime）
CHANGCI_MAP <- c(
  # 麦淘（规则化处理，见函数内）
  # 大小爱玩
  "11月17日 周五 19:30"    = "2023-11-17 19:30:00",
  "11月18日 周六 14:30"    = "2023-11-18 14:30:00",
  "11月18日 周六 19：30"   = "2023-11-18 19:30:00",
  "11月19日 周日 14:30"    = "2023-11-19 14:30:00",
  "11月19日 周日 19:30"    = "2023-11-19 19:30:00",
  "11月25日 周六 14:30"    = "2023-11-25 14:30:00",
  "11月25日 周六 19：30"   = "2023-11-25 19:30:00",
  "11月26日 周日 14:30"    = "2023-11-26 14:30:00",
  "11月24日 周五 19:30"    = "2023-11-24 19:30:00",
  "12月3日周日 14：30"     = "2023-12-03 14:30:00",
  "12月2日周六14：30"      = "2023-12-02 14:30:00",
  "12月1日周五19：30"      = "2023-12-01 19:30:00",
  "12月2日 周六 14:30"     = "2023-12-02 14:30:00",
  "12月2日周六19：30"      = "2023-12-02 19:30:00",
  "12月9日周六19:30"       = "2023-12-09 19:30:00",
  # 青盟
  "1.7 14:00"              = "2024-01-07 14:00:00",
  # 亲亲佑宝贝
  "12.03 - 14:30"          = "2023-12-03 14:30:00",
  "12.10  - 14:30"         = "2023-12-10 14:30:00",
  # 深圳儿童周末
  "1.27 14:30"             = "2024-01-27 14:30:00",
  # 亲友内购
  "11.18 19:30"            = "2023-11-18 19:30:00",
  "12.30 14:00"            = "2023-12-30 14:00:00",
  "11-21 19：30"           = "2023-12-30 14:00:00",
  "11.28 19：30"           = "2023-11-28 19:30:00",
  "12.2 19:30"             = "2023-12-02 19:30:00",
  "45259.8125"             = "2023-11-29 19:30:00",
  # 荧灿
  "12.31 19:30"            = "2023-12-31 19:30:00",
  # 百斯特-上海
  "12.9- 14:30"            = "2023-12-09 14:30:00",
  # 梦海文化
  "2023-11-18 周六 14:30"  = "2023-11-18 14:30:00",
  "11.18 19:30"            = "2023-11-18 19:30:00",
  "12.03 14:30"            = "2023-12-03 14:30:00",
  "12月9日19:30"           = "2023-12-09 19:30:00"
)

map_changci <- function(x, extra = character(0)) {
  all_map <- c(CHANGCI_MAP, extra)
  result  <- all_map[x]
  unknown <- is.na(result) & !is.na(x)
  if (any(unknown)) {
    warning("未知场次：", paste(unique(x[unknown]), collapse = " | "))
  }
  unname(result)
}

# =============================================================================
# 3. 数据源抽取（Extract）
# =============================================================================

extract_channel_orders <- function() {
  path <- file.path(DATA_DIR, "MA渠道订单总表.xlsx")
  sheets <- list(
    mt       = "麦淘（现场取票）",
    dxaw     = "大小爱玩（10% 现场取票）",
    `247bj`  = "247北京（10% 快递+现场取）",
    dhpw     = " 大河票务 快递",
    nbyg     = "内部员工",
    `247sh`  = "247 上海（10% 快递）",
    fmbbj    = "父母邦 北京（现场取票 10%）",
    qm       = "青盟（快递）",
    may      = "MAY 个人分销（10% 快递）",
    qyng     = "亲友内购",
    szlw     = "深圳遛娃指南（不选座 10% 现场取票）",
    szxd     = "秀动深圳（不选座 票提6% 快递）",
    djgj     = "大觉观剧（报单 现场取票 10）",
    szetzm   = "深圳儿童周末（手动报单 不选座 现场取票 10%）",
    shxd     = "秀动上海（不选座 6% 快递）",
    bjxd     = "秀动 北京 快递 6）",
    fmbsh    = "父母邦上海（10% 不选座 现场取票 按项目结算）",
    lxy      = "乐学营 （不选座 快递到付！！ 10%）",
    qmom     = "球妈（不选座 现场取票 10%）",
    thats_sz = "that's urban 深圳（10% 不选座 现场取票）",
    thats_bj = "that's urban 北京（不选座 10% 现场取票）",
    `247sz`  = "247 深圳（快递 10%）",
    qqybb    = "亲亲佑宝贝（北京 10% 现场取）",
    olwh     = "欧乐文化（10% 现场取）",
    fygmm    = "翻译官妈妈（北京 现场取 10%）",
    mhwh     = "梦海文化（现场取）",
    bstsh    = "MOR百斯特上海",
    ypzx     = "艺票在线（邮寄）",
    jzr      = "剧中人北京",
    zls      = "赵老师",
    ycsh     = "荧灿（上海MA）",
    thats_sh = "that's urban 上海"
  )
  map(sheets, function(s) {
    col_flag <- if (s == "内部员工") FALSE else TRUE
    df <- readxl::read_xlsx(path, sheet = s, col_names = col_flag)
    if (s == "内部员工") df <- df[, 1:3]
    df
  })
}

extract_szbh <- function(use_cache = TRUE) {
  cache_file   <- file.path(CACHE_DIR, "已读取-深圳滨海.xlsx")
  read_log     <- file.path(CACHE_DIR, "MA已读取文档列表.xlsx")
  already_read <- readxl::read_xlsx(read_log)$path

  files <- list.files(file.path(DATA_DIR, "深圳滨海"), full.names = TRUE)
  new_files <- setdiff(files, already_read)

  base <- if (use_cache && file.exists(cache_file)) {
    readxl::read_xlsx(cache_file)
  } else NULL

  if (length(new_files) == 0) return(base)

  pat_date  <- regex("场次明细报表(\\d{2}-\\d{2})\\.xlsx$")
  col_std   <- c("销售政策", "数值类型", "1280", "1080", "880", "680", "480", "280", "180")

  # 列索引因文件版本不同而异
  COL_IDX_V1 <- c(3, 5, 6, 7, 8, 10, 13, 15, 16)   # 01-26 特殊版
  COL_IDX_V2 <- c(3, 6, 7, 9, 11, 14, 17, 19, 20)  # 常规版

  SHOW_NAME  <- "伦敦西区原版音乐剧《玛蒂尔达》"
  VENUE_NAME <- "深圳滨海艺术中心 歌剧厅"

  parse_one_szbh <- function(path) {
    raw     <- readxl::read_xlsx(path, sheet = "场次明细报表",
                                 col_names = FALSE, guess_max = 2000) |>
               dplyr::select(where(is.character))
    is_v1   <- str_detect(path, "01-26\\.xlsx$")
    col_idx <- if (is_v1) COL_IDX_V1 else COL_IDX_V2

    # 场次列和渠道列索引
    cc_col  <- if (is_v1) 14L else 18L
    qd_col  <- 4L

    body <- raw |>
      dplyr::select(all_of(col_idx)) |>
      setNames(col_std) |>
      filter(!is.na(数值类型), !is.na(`1280`)) |>
      mutate(销售政策 = if_else(数值类型 == "数量" & is.na(销售政策), "合计", 销售政策))

    # 提取场次和渠道列表
    list_cc <- as.character(raw[[cc_col]])
    list_qd <- as.character(raw[[qd_col]])

    # 把下一行的渠道补填到场次行（处理合并单元格）
    scene_rows <- which(!list_cc %in% c(SHOW_NAME, NA))
    raw[scene_rows + 2, qd_col] <- coalesce(list_qd[scene_rows + 2], "无")
    list_qd <- as.character(raw[[qd_col]])

    cc_clean <- list_cc[!list_cc %in% c(SHOW_NAME, NA)]
    qd_clean <- list_qd[!list_qd %in% c(SHOW_NAME, VENUE_NAME, NA)]
    cc_parsed <- as.character(
      as.POSIXct(cc_clean, format = "%Y年%m月%d日 %H:%M")
    )

    # 每个场次/渠道对应的行数
    total_rows <- diff(c(-1L, which(body$销售政策 == "合计")))

    body |>
      fill(销售政策) |>
      mutate(across(`1280`:`180`, strip_commas)) |>
      mutate(
        渠道类型 = rep(qd_clean, total_rows),
        场次     = rep(cc_parsed, total_rows)
      ) |>
      filter(!销售政策 %in% c("小计", "合计")) |>
      pivot_longer(`1280`:`180`, names_to = "票价", values_to = "数值") |>
      pivot_wider(names_from = 数值类型, values_from = 数值) |>
      rename(张数 = 数量, 实收 = 金额) |>
      mutate(
        项目   = "深圳MA",
        剧场   = VENUE_NAME,
        渠道明细 = 销售政策,
        票价   = as.numeric(票价),
        下单时间 = {
          mmdd <- str_match(path, "场次明细报表(\\d{2}-\\d{2})")[, 2]
          mm   <- as.integer(str_split_fixed(mmdd, "-", 2)[, 1])
          yr   <- if_else(mm > 3, 2023L, 2024L)
          as.POSIXct(sprintf("%d-%s 00:00:00", yr, mmdd), tz = "UTC")
        }
      ) |>
      filter(张数 != 0 | 实收 != 0)
  }

  new_data <- map_dfr(new_files, parse_one_szbh)
  bind_rows(base, new_data)
}

extract_bj_damai <- function() {
  cache <- file.path(BASE_DIR, "数据/da_bj_dm.xlsx")
  if (file.exists(cache)) return(readxl::read_xlsx(cache))

  files <- list.files(file.path(DATA_DIR, "北京大麦"), full.names = TRUE)
  raw <- map_dfr(files, function(f) {
    tmp <- readxl::read_xlsx(f, col_types = rep("text", 17), col_names = FALSE)
    tmp[-c(1:5), -1] |>
      mutate(
        项目名称 = as.character(tmp[1, 2]),
        场次名称 = as.character(tmp[2, 2]),
        演出场馆 = as.character(tmp[3, 2])
      ) |>
      setNames(c(
        "票单号码", "大麦订单号", "B端订单号",
        "票品名称", "票品金额", "实收金额", "优惠金额",
        "优惠政策", "看台", "楼层", "排号", "座号",
        "购票人", "操作人", "售出时间", "备注",
        "项目名称", "场次名称", "演出场馆"
      ))
  })
  result <- raw |> filter(str_detect(票单号码, "^\\d+$"))
  write.xlsx(result, cache)
  result
}

extract_bj_maoyuan <- function() {
  readxl::read_xls(
    file.path(DATA_DIR, "北京猫眼/订单明细表.xls")
  )
}

extract_douyin <- function() {
  read_dir(file.path(DATA_DIR, "抖音"))
}

# =============================================================================
# 4. 数据转换（Transform）— 各渠道
# =============================================================================

# ---------- 4-1 深圳滨海 ----------
transform_szbh <- function(raw) {
  EXCLUDE_CHANNELS <- c("七幕人生文化产业（北京）有限公司", "深圳滨海艺术中心")

  lookup_channel_type <- function(x) {
    result <- rep(NA_character_, length(x))
    for (type in names(SZBH_CHANNEL_MAP)) {
      result[x %in% SZBH_CHANNEL_MAP[[type]]] <- type
    }
    unknown <- is.na(result) & !is.na(x)
    if (any(unknown)) warning("深圳滨海未知渠道：", paste(unique(x[unknown]), collapse = " | "))
    result
  }

  raw |>
    filter(!渠道类型 %in% EXCLUDE_CHANNELS) |>
    mutate(
      渠道类型 = lookup_channel_type(渠道类型),
      剧目     = "MA"
    ) |>
    pad_cols()
}

# ---------- 4-2 北京大麦 ----------
transform_bj_damai <- function(raw) {
  EXCLUDE_OPERATORS <- c(
    "七幕人生文化产业(北京)有限公司管理员",
    "中国铁路文工团管理员",
    "qmrs003"
  )

  raw |>
    filter(!操作人 %in% EXCLUDE_OPERATORS) |>
    mutate(
      票价     = lookup_price(票品名称),
      张数     = 1L,
      实收     = as.numeric(实收金额),
      场次     = {
        pat <- "(\\d{4}-\\d{2}-\\d{2}).*?(\\d{2}):(\\d{2})"
        m   <- str_match(场次名称, pat)
        paste0(m[, 2], " ", m[, 3], ":", m[, 4], ":00")
      },
      下单时间 = as.POSIXct(售出时间, tz = "UTC"),
      渠道类型 = "大麦",
      渠道明细 = "北京大麦",
      剧目     = "MA"
    ) |>
    pad_cols()
}

# ---------- 4-3 北京猫眼 ----------
transform_bj_maoyuan <- function(raw) {
  raw |>
    filter(项目名称 == "伦敦西区原版音乐剧《玛蒂尔达》 Matilda THE MUSICAL") |>
    mutate(
      剧目     = "MA",
      场次     = as.character(演出时间),
      下单时间 = as.POSIXct(下单时间, tz = "UTC"),
      渠道类型 = "票务网站",
      渠道明细 = "猫眼",
      票价     = as.numeric(票面价),
      实收     = (as.numeric(总售价) + as.numeric(配送费)) * 0.9,
      张数     = as.numeric(票张数),
      收货人姓名 = 收件人,
      收货人手机 = as.character(收件人电话),
      地址     = 收件人地址
    ) |>
    pad_cols()
}

# ---------- 4-4 抖音 ----------
transform_douyin <- function(raw) {
  DOUYIN_PROJ_MAP <- c(
    "伦敦西区原版音乐剧《玛蒂尔达》北京站【新东方直播间】" = "抖音-新东方直播间",
    "伦敦西区原版音乐剧《玛蒂尔达》北京站"               = "抖音"
  )
  DOUYIN_CHANGCI_MAP <- c(
    "2023.11.25 周六 14:30" = "2023-11-25 14:30:00",
    "2023.12.2 周六 14:30"  = "2023-12-02 14:30:00",
    "2023.11.22 周三 19:30" = "2023-11-22 19:30:00"
  )

  raw |>
    fill(everything()) |>
    mutate(
      剧目     = "MA",
      场次     = DOUYIN_CHANGCI_MAP[场次],
      下单时间 = as.POSIXct(支付时间, tz = "UTC"),
      渠道类型 = "分销-报单",
      渠道明细 = DOUYIN_PROJ_MAP[项目],
      票价     = as.numeric(票价),
      实收     = 票价 * 0.9,
      张数     = 1L,
      收货人姓名 = 收件人,
      收货人手机 = as.character(收件人手机号),
      地址     = 收件人地址
    ) |>
    pad_cols()
}

# ---------- 4-5 分销报单渠道（统一框架） ----------
# 每个渠道提供一个"转换函数"，signature: data.frame -> data.frame (标准列)

make_xd_transformer <- function(channel_name, commission_rate = 0.94) {
  pat <- "(\\d{4}-\\d{2}-\\d{2}).{4}(\\d{2}:\\d{2})"
  function(raw) {
    raw |>
      mutate(
        场次     = paste0(str_match(场次, pat)[, 2], " ",
                         str_match(场次, pat)[, 3], ":00"),
        渠道类型 = "分销-报单",
        渠道明细 = channel_name,
        票价     = as.numeric(票品),
        张数     = as.integer(数量),
        实收     = 票品 * 数量 * commission_rate
      ) |>
      pad_cols()
  }
}

transform_mt <- function(raw) {
  # 麦淘场次：字符串以月份开头，>1 则是 2024，否则 2023
  parse_mt_changci <- function(x) {
    yr <- if_else(str_sub(x, 1, 2) > "01", "2023-", "2024-")
    as.POSIXct(paste0(yr, x, ":00"), tz = "UTC")
  }

  raw |>
    mutate(
      场次     = as.character(parse_mt_changci(
                   str_match(场次名称, "([0-9 -:]+[0-9])")[, 2])),
      票价     = lookup_price(套餐名称),
      张数     = lookup_qty_multiplier(售卖数量),
      渠道类型 = "分销-报单",
      渠道明细 = "麦淘",
      下单时间 = as.POSIXct(付款时间, tz = "UTC"),
      实收     = as.numeric(底价),
      收货人姓名 = 订单联系人,
      收货人手机 = as.character(手机)
    ) |>
    pad_cols()
}

transform_dxaw <- function(raw) {
  pat_spec <- "^(.*?)-(.*?)$"
  raw |>
    filter(!is.na(商品规格)) |>
    mutate(
      场次_raw  = str_match(商品规格, pat_spec)[, 2],
      规格_raw  = str_match(商品规格, pat_spec)[, 3],
      场次     = map_changci(场次_raw),
      票价     = lookup_price(规格_raw),
      张数     = as.integer(商品数量) * lookup_qty_multiplier(规格_raw),
      实收     = as.numeric(商品金额小计) * 0.9,
      下单时间 = as.POSIXct(买家付款时间, tz = "UTC"),
      渠道类型 = "分销-报单",
      渠道明细 = "大小爱玩",
      收货人姓名 = `收货人/提货人`,
      收货人手机 = as.character(`收货人手机号/提货人手机号`),
      地址     = `详细收货地址/提货地址`
    ) |>
    pad_cols()
}

# 247 系列（北京/上海/深圳）共用价格解析
transform_247 <- function(raw, channel_name, year = 2023L) {
  parse_247_date <- function(x) {
    m <- str_match(x, "(\\d+)\\.(\\d+)")
    as.POSIXct(sprintf("%d-%02s-%02s 00:00:00",
                       year, m[, 2], m[, 3]), tz = "UTC")
  }

  raw |>
    fill(everything()) |>
    filter(!is.na(售卖日期)) |>
    mutate(
      场次     = as.character(`Session 1`),
      下单时间 = parse_247_date(售卖日期),
      渠道类型 = "分销-报单",
      渠道明细 = channel_name,
      票价     = lookup_price(as.character(`Price 1`)),
      张数     = as.integer(`Quantity 1`) * lookup_qty_multiplier(as.character(`Price 1`)),
      实收     = as.numeric(price) * 0.9,
      收货人姓名 = Receiver,
      收货人手机 = as.character(Phone),
      地址     = as.character(`Full address`)
    ) |>
    pad_cols()
}

transform_fmb <- function(raw, channel_name = "父母邦-北京") {
  pat <- "票种：(\\d+)档(.*?)，使用时间：(.*)"
  qty_map <- c("单人票3张" = 3L, "单人票2张" = 2L, "单人票" = 1L)

  raw |>
    mutate(
      票价     = as.numeric(str_match(商品说明, pat)[, 2]),
      张数     = qty_map[str_match(商品说明, pat)[, 3]] * as.integer(数量),
      场次     = str_match(商品说明, pat)[, 4],
      渠道类型 = "分销-报单",
      渠道明细 = channel_name,
      实收     = as.numeric(结算价小计),
      收货人姓名 = 用户姓名
    ) |>
    pad_cols()
}

# 标准化票档报单渠道（票档/张数/总金额 列名已统一）
make_standard_transformer <- function(channel_name, changci_map,
                                      commission = 1.0) {
  function(raw) {
    raw |>
      mutate(
        场次     = map_changci(场次, changci_map),
        下单时间 = as.POSIXct(售卖时间, tz = "UTC"),
        渠道类型 = "分销-报单",
        渠道明细 = channel_name,
        票价     = as.numeric(票档),
        张数     = as.integer(张数),
        实收     = as.numeric(总金额) * commission,
        收货人姓名 = 姓名,
        收货人手机 = as.character(手机号)
      ) |>
      pad_cols()
  }
}

# 使用工厂函数批量生成简单渠道的 transformer
SIMPLE_CHANNEL_CONFIG <- list(
  qm     = list(name = "青盟",     extra = c("1.7 14:00" = "2024-01-07 14:00:00")),
  qqybb  = list(name = "亲亲佑宝贝", extra = c(
                  "12.03 - 14:30" = "2023-12-03 14:30:00",
                  "12.10  - 14:30"= "2023-12-10 14:30:00")),
  szetzm = list(name = "深圳儿童周末", extra = c("1.27 14:30" = "2024-01-27 14:30:00")),
  bstsh  = list(name = "百斯特-上海", extra = c("12.9- 14:30" = "2023-12-09 14:30:00")),
  ypzx   = list(name = "艺票在线",  extra = c(
                  "11月28日 19:30" = "2023-11-28 19:30:00",
                  "11月29日 19:30" = "2023-11-29 19:30:00",
                  "11月30日 19:30" = "2023-11-30 19:30:00",
                  "12月1日 19:30"  = "2023-12-01 19:30:00",
                  "12月8日 19:30"  = "2023-12-08 19:30:00")),
  jzr    = list(name = "剧中人北京", extra = c(
                  "11月28日 19:30" = "2023-11-28 19:30:00",
                  "11月29日19:30"  = "2023-11-29 19:30:00",
                  "11月30日 19:30" = "2023-11-30 19:30:00",
                  "12月1日 19:30"  = "2023-12-01 19:30:00",
                  "12月8日 19:30"  = "2023-12-08 19:30:00")),
  ycsh   = list(name = "荧灿",      extra = c("12.31 19:30" = "2023-12-31 19:30:00"))
)

# =============================================================================
# 5. 主 ETL 入口
# =============================================================================

run_etl <- function(update_cache = FALSE) {
  message("=== [Extract] 读取原始数据 ===")

  ch_orders <- extract_channel_orders()
  raw_szbh  <- extract_szbh(use_cache = !update_cache)
  raw_bjdm  <- extract_bj_damai()
  raw_bjmy  <- extract_bj_maoyuan()
  raw_dy    <- extract_douyin()

  message("=== [Transform] 清洗各渠道数据 ===")

  layers <- list(
    # 剧院渠道
    transform_szbh(raw_szbh),
    transform_bj_damai(raw_bjdm),

    # 票务网站
    transform_bj_maoyuan(raw_bjmy),
    transform_douyin(raw_dy),

    # 分销报单
    transform_mt(ch_orders$mt),
    transform_dxaw(ch_orders$dxaw),
    transform_247(ch_orders$`247bj`, "247-北京"),
    transform_247(ch_orders$`247sh`, "247-上海"),
    transform_247(ch_orders$`247sz`, "247-深圳"),
    transform_fmb(ch_orders$fmbbj, "父母邦-北京"),

    # 秀动三城复用同一工厂
    make_xd_transformer("秀动")(ch_orders$szxd),
    make_xd_transformer("秀动")(ch_orders$bjxd),
    make_xd_transformer("秀动")(ch_orders$shxd),

    # 简单标准渠道
    imap(SIMPLE_CHANNEL_CONFIG, function(cfg, key) {
      make_standard_transformer(cfg$name, cfg$extra)(ch_orders[[key]])
    }) |> bind_rows()
  )

  message("=== [Load] 合并为统一宽表 ===")

  da_all_ma <- bind_rows(layers) |>
    mutate(
      剧目   = coalesce(剧目, "MA"),
      张数   = as.integer(张数),
      票价   = as.numeric(票价),
      实收   = as.numeric(实收),
      下单时间 = as.POSIXct(下单时间, tz = "UTC"),
      场次   = as.character(场次)
    ) |>
    filter(!is.na(场次), 张数 > 0) |>
    arrange(下单时间)

  if (update_cache) {
    write.xlsx(da_all_ma,
               file.path(OUTPUT_DIR, "da_all_ma.xlsx"),
               rowNames = FALSE)
    message("已写出缓存：", file.path(OUTPUT_DIR, "da_all_ma.xlsx"))
  }

  da_all_ma
}

# =============================================================================
# 6. 下游分析函数（基于标准宽表）
# =============================================================================

# 日报汇总：当日各渠道售票/收入
daily_summary <- function(da, date_today) {
  date_tomorrow <- date_today + days(1)

  da |>
    filter(下单时间 >= date_today, 下单时间 < date_tomorrow) |>
    mutate(渠道大类 = case_when(
      渠道类型 %in% c("分销-系统", "分销-报单") ~ "分销",
      TRUE ~ 渠道类型
    )) |>
    group_by(渠道大类, 项目) |>
    summarise(售票数 = sum(张数, na.rm = TRUE),
              收入   = sum(实收, na.rm = TRUE),
              .groups = "drop")
}

# 场次累计：各场次/票档 售出数量与收入
session_summary <- function(da) {
  da |>
    group_by(项目, 场次, 票价) |>
    summarise(售票数 = sum(张数, na.rm = TRUE),
              收入   = sum(实收, na.rm = TRUE),
              .groups = "drop") |>
    arrange(场次, 票价)
}

# 项目累计收入（含/不含大客户）
project_total <- function(da, exclude_bulk = FALSE) {
  if (exclude_bulk) da <- filter(da, 渠道类型 != "大客户")
  da |>
    group_by(项目) |>
    summarise(收入 = sum(实收, na.rm = TRUE), .groups = "drop")
}

# 营销动作效果
action_performance <- function(da, date_today) {
  date_tomorrow <- date_today + days(1)

  da |>
    filter(下单时间 >= date_today, 下单时间 < date_tomorrow,
           渠道类型 == "自营") |>
    mutate(动作类型 = case_when(
      动作类型 %in% c("推文-开票", "推文-仅销售", "推文-含品宣") ~ "推文",
      TRUE ~ coalesce(动作类型, "自然销售")
    )) |>
    group_by(动作日期, 渠道明细, 动作类型) |>
    summarise(订单数 = n_distinct(paste(下单时间, 收货人手机)),
              售票数 = sum(张数, na.rm = TRUE),
              收入   = sum(实收, na.rm = TRUE),
              .groups = "drop") |>
    arrange(动作类型, desc(收入))
}

# =============================================================================
# 7. 执行（取消注释使用）
# =============================================================================

# da_all_ma  <- run_etl(update_cache = FALSE)
# today      <- as.POSIXct("2024-01-21 00:00:00", tz = "UTC")
#
# daily_summary(da_all_ma, today)
# project_total(da_all_ma)
# project_total(da_all_ma, exclude_bulk = TRUE)
# session_summary(da_all_ma)
# action_performance(da_all_ma, today)
