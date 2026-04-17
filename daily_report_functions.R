# =============================================================================
# daily_report_functions.R
# Utility functions for the daily ticketing report pipeline.
# Covers: referral-code attribution, project/venue mapping, inventory lookup,
#         distributor price parsing, and ticket-revenue calculations.
# =============================================================================

library(tidyverse)
library(readxl)
library(lubridate)
library(openxlsx)

# =============================================================================
# 0. Global configuration
# =============================================================================

date_today     <- as.POSIXct("2024-01-30 00:00:00", tz = "UTC")
date_tomorrow  <- date_today + 3600 * 24

# =============================================================================
# 1. Missing referral-code patch
#    During a known system outage some orders were saved without a referral
#    code. This lookup table maps order IDs back to the correct code.
# =============================================================================

fenxiao_replace <- readxl::read_xlsx(
  "D:/实习工作-交接/日更数据/七幕-分销码补充.xlsx",
  sheet = 2, col_names = TRUE
) |>
  dplyr::select(-...3) |>
  filter(!is.na(分销码)) |>
  distinct(订单号, 分销码)

fenxiao_replace_func <- function(order_ids, codes) {
  # For each order in the patch list, overwrite its code
  for (i in seq_along(order_ids)) {
    idx <- which(fenxiao_replace$订单号 == order_ids[i])
    if (length(idx)) codes[i] <- fenxiao_replace$分销码[idx]
  }
  codes
}

# =============================================================================
# 2. Data ingestion
# =============================================================================

# Column type overrides for the seated-ticket CSV
list_tmp        <- rep(list("?"), 50)
list_tmp[c(2:3, 6:9, 28)] <- "c"

# Column type overrides for the standing/general-admission CSV
list_tmp2       <- rep(list("?"), 46)
list_tmp2[c(2:3, 9)] <- "c"

da <- bind_rows(
  read_csv("D:/选座明细.csv",    show_col_types = FALSE, col_types = list_tmp),
  read_csv("D:/选座明细8-9.csv", show_col_types = FALSE, col_types = list_tmp),
  read_csv("D:/选座明细10.csv",  show_col_types = FALSE, col_types = list_tmp),
  read_csv("D:/选座明细11.csv",  show_col_types = FALSE, col_types = list_tmp),
  read_csv("D:/选座明细12.csv",  show_col_types = FALSE, col_types = list_tmp)
) |>
  mutate(
    收货人手机 = as.character(收货人手机),
    下单时间   = as.POSIXct(下单时间, tz = "UTC")
  ) |>
  # Patch missing codes
  mutate(分销码 = fenxiao_replace_func(订单号, 分销码)) |>
  # Identify "ikids" half-price promo (50 % discount, post 2023-12-03)
  mutate(分销码 = if_else(
    优惠金额 == 0.5 * 票价 &
      下单时间 >= as.POSIXct("2023-12-03 00:00:00", tz = "UTC") &
      分销码 == "-",
    "ikids", 分销码
  )) |>
  filter(来源 != "线下票台") |>
  # Exclude "近乎正常" productions (different reporting scope)
  filter(!str_detect(项目, "近乎正常"))

da_fenxiao <- read_csv(
  "D:/实习工作-交接/日更数据/展览活动明细.csv",
  show_col_types = FALSE, col_types = list_tmp2
) |>
  # Manually tag a bulk-order account that uses a dedicated project name
  mutate(分销码 = if_else(项目 == "【玛蒂尔达】-上海德威学校", "大客户-上海德威学校", 分销码)) |>
  filter(项目 != "《玛蒂尔达》上海站-宏文学校")

# WeChat article read-count exports (subscription + service account)
da_read <- bind_rows(
  readxl::read_xls("D:/1006104843_1702954051_3_19.xls"),
  readxl::read_xls("D:/2023-07~08.xls"),
  readxl::read_xls("D:/2023-09.xls"),
  readxl::read_xls("D:/2023-10.xls")
)
da_read_fwh <- readxl::read_xls(
  "D:/实习工作-交接/日更数据/服务号阅读量/3375050864_1702954115_3_19.xls"
)

# =============================================================================
# 3. Article title → short-name mapping (mark_abbr)
#    Used to attach human-readable labels to push articles when computing
#    article-driven ticket revenue.
# =============================================================================

mark_abbr <- function(text, from) {
  ans <- text

  # --- Service account (服务号) articles ---
  if_svc <- function(title, label) {
    ans[text == title & from == "服务号"] <<- label
  }
  if_svc("关于音乐剧《摇滚莫扎特》中文版预演场限量黑胶、CD延迟邮寄的公告", "服务号推文-黑胶公告")
  if_svc("北京站圆满收官｜英国国宝级音乐剧《玛蒂尔达》绝佳口碑燃爆京城",   "服务号推文-MA北京收官")
  if_svc("公主请保存！英国国宝级音乐剧《玛蒂尔达》歌词壁纸大放送！",       "服务号推文-歌词壁纸")
  if_svc("招募｜伦敦西区原版音乐剧《玛蒂尔达》北京站外场志愿者招募",       "服务号推文-志愿者招募")
  if_svc("全票档开票｜伦敦西区原版音乐剧《玛蒂尔达》北京站三轮全票档开票&卡司官宣", "服务号推文-MA三轮全票档")
  if_svc("三轮早鸟开票｜在这个小女孩身上，寻找一些久违的勇气",             "服务号推文-MA三轮早鸟")
  if_svc("剧团助理招聘｜伦敦西区原版音乐剧《玛蒂尔达》2023中国巡演季",     "服务号推文-MA中国巡演")
  if_svc("官方主题曲《沉睡在玫瑰上》发布！音乐剧《摇滚莫扎特》中文版剩余场次早鸟全开！", "服务号推文-MOR二轮早鸟")
  if_svc("二轮全票档开票｜伦敦西区原版音乐剧《玛蒂尔达》尊享体验工作坊全新上线", "服务号推文-MA二轮全票档&工作坊")
  if_svc("二轮早鸟开票｜伦敦西区原版音乐剧《玛蒂尔达》北京、上海站火热售卖中", "服务号推文-MA二轮早鸟开票")
  if_svc("一键购票｜中文版《摇滚莫扎特》& 英文原版《玛蒂尔达》热卖中",     "服务号推文-长期次条MA&MOR")

  # --- Subscription account (订阅号) articles ---
  # (Long-running "buy now" evergreen articles)
  evergreen_titles <- c(
    "一键购票｜英文原版《玛蒂尔达》热卖中",
    "一键购票｜中文版《摇滚莫扎特》&英文原版《玛蒂尔达》热卖中",
    "一键购票｜英文原版《玛蒂尔达》热卖中&中文版《摇滚莫扎特》本周日收官",
    "一键购票｜英文原版《玛蒂尔达》&中文版《摇滚莫扎特》热卖中",
    "一键购票｜中文版《摇滚莫扎特》预演场尚有少量余票&英文原版《玛蒂尔达》热卖中",
    "一键购票｜中文版《摇滚莫扎特》& 英文原版《玛蒂尔达》热卖中",
    "一键购票｜ 英文原版《玛蒂尔达》&音乐剧《摇滚莫扎特》中文版热卖中"
  )
  ans[is.element(text, evergreen_titles) & from == "订阅号"] <- "推文-长期次条MA&MOR"

  if_sub <- function(title, label) {
    ans[text == title & from == "订阅号"] <<- label
  }
  if_sub("会昌戏剧节｜跟领导请好假了，元旦过后就到这里看戏！！",                 "推文-会昌戏剧节")
  if_sub("倒计时3天｜《玛蒂尔达》北京站29场售罄，上海站新场次明日开票",          "推文-MA最后3天")
  if_sub("倒计时4天｜北京最后一周极少量Rush票现场售卖！",                        "推文-MA最后4天")
  if_sub("倒计时5天｜和伦敦西区原版音乐剧《玛蒂尔达》北京站现场见！",            "推文-MA最后5天")

  ans
}

# =============================================================================
# 4. Production identifier (mark_musicals)
#    Maps project name strings to short production codes (MA / MOR / NTN / test)
# =============================================================================

mark_musicals <- function(project) {
  l   <- length(project)
  ans <- rep("未知", l)

  ans[is.element(project, c(
    "【北京站】七幕人生出品 百老汇摇滚音乐剧《近乎正常》中文版",
    "【上海站】七幕人生出品 百老汇摇滚音乐剧《近乎正常》中文版",
    "【广州站】七幕人生出品 百老汇摇滚音乐剧《近乎正常》中文版",
    "【深圳站】七幕人生出品 百老汇摇滚音乐剧《近乎正常》中文版"
  ))] <- "NTN"

  ans[is.element(project, c(
    "【上海站】七幕人生出品法国现象级音乐剧《摇滚莫扎特》中文版 正式场",
    "七幕人生出品 法国现象级音乐剧《摇滚莫扎特》中文版 预演场"
  ))] <- "MOR"

  ans[is.element(project, c(
    "【上海站】伦敦西区原版音乐剧《玛蒂尔达》",
    "【北京站】伦敦西区原版音乐剧《玛蒂尔达》",
  ))] <- "MA"

  ans[is.element(project, c("测试选座项目", "测试站票项目", "测试"))] <- "test"

  if (any(ans == "未知")) {
    print(unique(project[ans == "未知"]))
    warning("mark_musicals: unrecognised project name(s)")
  }
  ans
}

# =============================================================================
# 5. Referral-code attribution (mark_from)
#    Maps a referral-code string + order timestamp to:
#      [1] channel label (渠道明细)
#      [2] action date   (动作日期)
#      [3] channel type  (动作类型)
#
#    Codes are grouped into: natural sales, distributor, bulk-order (大客户),
#    SMS campaign, WeChat 1-to-1, article push, Weibo, enterprise-WeChat group,
#    enterprise-WeChat moments, official-account display, paid ads, and misc.
# =============================================================================

mark_from <- function(text, time) {
  l     <- length(text)
  ans_m <- matrix(rep(c("其他", "", "其他"), l), 3, l)

  # Helper: set all 3 rows for matching indices
  set3 <- function(mask, label, date = "", type) {
    ans_m[1, mask] <<- label
    ans_m[2, mask] <<- date
    ans_m[3, mask] <<- type
  }
  set1 <- function(mask, label, type) {
    ans_m[1, mask] <<- label
    ans_m[3, mask] <<- type
  }

  # ------------------------------------------------------------------
  # 5-1  Natural / organic sales
  # ------------------------------------------------------------------
  organic_codes <- c(
    "WTtxTtPLdpRb",
    "JsfLQxhfGRr5%3A1667358556277%3A15c297bd%3A1667358556277",
    "JsfLQxhfGRr5%3A1676624984968%3A7bed0f9d%3A1676624984968",
    "4WH3svVSAy6N",
    "JsfLQxhfGRr5%3A1676551334092%3A3cf62dec%3A1676551334092",
    "-"
  )
  set1(is.element(text, organic_codes), "自然销售", "自然销售")
  set1(is.element(text, c("hunzaijuchang", "FXvHPvSH")) &
         time < as.POSIXct("2023-09-05", tz = "UTC"), "自然销售", "自然销售")
  set1(is.element(text, "dajueguanju"), "自然销售", "自然销售")

  # ------------------------------------------------------------------
  # 5-2  Distributors (分销)
  #      New distributors: also add to mark_piaoti_zy (default 0.9 rebate)
  # ------------------------------------------------------------------
  set1(text == "FXvZLSvMAvSHvYJ",                       "分销-a",           "分销")
  set1(text == "FXvMNKXJvMAvSHvZZ",                     "分销-b",   "分销")
  set1(text == "FXvZLSvMAvSHvCG",                       "分销-c",           "分销")

  # ------------------------------------------------------------------
  # 5-3  Bulk / corporate orders (大客户)
  # ------------------------------------------------------------------
  bulk <- list(
    "KHvWDLYYEYvMAvSHvYJ"   = "大客户-a",
    "KHvSZHCTvMAvSHvZZ"     = "大客户-b"
  )
  for (code in names(bulk)) {
    set1(text == code, bulk[[code]], "大客户")
  }

  # ------------------------------------------------------------------
  # 5-4  SMS campaigns (短信)
  # ------------------------------------------------------------------
  sms <- list(
    list("QMvDXvYXvNTNvALLvAa",  "短信-NTN第一轮早鸟开票-银星",     "2024-01-23"),
    list("QMvDXvJXvNTNvALLvAa",  "短信-NTN第一轮早鸟开票-金星",     "2024-01-23")
  )
  for (s in sms) set3(text == s[[1]], s[[2]], s[[3]], "短信")

  # Early batches identified by time window rather than unique code
  set3(text == "zaoniaoduanxin" & time < as.POSIXct("2023-08-21 10:00:00", tz = "UTC"),
       "短信", "2023-08-17", "短信")
  set3(text == "zaoniaoduanxin" &
         time >= as.POSIXct("2023-08-21 10:00:00", tz = "UTC") &
         time <  as.POSIXct("2023-08-23 15:00:00", tz = "UTC"),
       "短信", "2023-08-21", "短信")
  set3(text == "zaoniaoduanxin" & time >= as.POSIXct("2023-08-23 15:00:00", tz = "UTC"),
       "短信", "2023-08-24", "短信")
  set3(text == "QMvDXvGvSH" & time >= as.POSIXct("2023-08-27", tz = "UTC"),
       "短信-上海", "2023-08-27", "短信")
  set3(text == "QMvDXvGvBJ" & time >= as.POSIXct("2023-08-27", tz = "UTC"),
       "短信-北京", "2023-08-27", "短信")
  set3(text == "QMvDXvGvSZ" &
         time >= as.POSIXct("2023-08-27", tz = "UTC") &
         time <  as.POSIXct("2023-09-03", tz = "UTC"),
       "短信-广深", "2023-08-27", "短信")

  # ------------------------------------------------------------------
  # 5-5  WeChat 1-to-1 (小七点对点)
  # ------------------------------------------------------------------
  ddd <- list(
    list("QMvDDDvNTNvALLvAa",     "小七点对点-NTN第一轮早鸟开票", "2024-01-23"),
    list("QMvXQvMAvDA",           "小七点对点-MA四轮早鸟",        "2023-12-08"),
    list("QMvPXvMAvCC",           "培训点对点-MA三轮早鸟提醒",    "2023-11-13"),
    list("QMvXQvMAvCB",           "小七点对点-MA三轮全票档",      "2023-11-14"),
    list("QMvXQvMAvC",            "小七点对点-MA三轮早鸟",        "2023-11-08"),
    list("QMvDDDvMAvBJ",          "小七点对点-MA三轮早鸟",        "2023-11-08"),
    list("QMvXQvMORvC",           "小七点对点-MOR二轮全票档",     "2023-10-27"),
    list("QMvXQvMORvB",           "小七点对点-MOR二轮早鸟",       "2023-10-20"),
    list("QMvXQvMAvBvBJ",         "小七点对点-MA二轮全票档-北京", "2023-10-18"),
    list("QMvXQPXDDDvMAvALL",     "小七点对点-MA二轮早鸟-培训",  "2023-10-12"),
    list("QMvXQvCvMAvSHvA",       "小七点对点-MA二轮早鸟-上海",  "2023-10-11"),
    list("QMvXQvCvMAvBJvA",       "小七点对点-MA二轮早鸟-北京",  "2023-10-11"),
    list("QMvXQvCvMAvQBDvSZ",     "小七点对点-MA全票档-广深",    "2023-09-20"),
    list("QMvXQvCvMAvQBDvSH",     "小七点对点-MA全票档-上海",    "2023-09-20"),
    list("QMvXQvCvMAvQBDvBJ",     "小七点对点-MA全票档-北京",    "2023-09-20"),
    list("QMvXQvCvMAvSH",         "小七点对点-MA早鸟-上海",       "2023-09-13"),
    list("QMvXQvCvMAvSZ",         "小七点对点-MA早鸟-深圳",       "2023-09-13"),
    list("QMvXQvCvMAvBJ",         "小七点对点-MA早鸟-北京",       "2023-09-13")
  )
  for (d in ddd) {
    set3(text == d[[1]], d[[2]], d[[3]], "小七点对点")
  }

  # Early batches (time-window disambiguation)
  set3(text == "ziyingdianduidian" & time < as.POSIXct("2023-08-18 10:00:00", tz = "UTC"),
       "小七点对点", "2023-08-17", "小七点对点")
  set3(text == "ziyingdianduidian" &
         time >= as.POSIXct("2023-08-18 10:00:00", tz = "UTC") &
         time <  as.POSIXct("2023-08-21 10:00:00", tz = "UTC"),
       "小七点对点", "2023-08-18", "小七点对点")
  set3(text == "ziyingdianduidian" &
         time >= as.POSIXct("2023-08-21 10:00:00", tz = "UTC") &
         time <  as.POSIXct("2023-08-23 15:00:00", tz = "UTC"),
       "小七点对点", "2023-08-21", "小七点对点")
  set3(text == "ziyingdianduidian" & time >= as.POSIXct("2023-08-23 15:00:00", tz = "UTC"),
       "小七点对点", "2023-08-23", "小七点对点")
  set3(text == "QMvXQvGvSH" & time >= as.POSIXct("2023-08-27", tz = "UTC"),
       "小七点对点-上海", "2023-08-27", "小七点对点")
  set3(text == "QMvXQvGvSZ" &
         time >= as.POSIXct("2023-08-27", tz = "UTC") &
         time <  as.POSIXct("2023-08-29", tz = "UTC"),
       "小七点对点-深圳", "2023-08-27", "小七点对点")
  set3(text == "QMvXQvGvBJZSC" &
         time >= as.POSIXct("2023-08-27", tz = "UTC") &
         time <  as.POSIXct("2023-09-02", tz = "UTC"),
       "小七点对点-北京-正式场", "2023-08-27", "小七点对点")
  set3(text == "QMvXQvGvBJZSC" & time >= as.POSIXct("2023-09-02", tz = "UTC"),
       "小七点对点-北京-正式场", "2023-09-02", "小七点对点")
  set3(text == "QMvXQvGvSZ" & time >= as.POSIXct("2023-09-06", tz = "UTC"),
       "小七点对点-深圳-全票档", "2023-09-06", "小七点对点")

  # ------------------------------------------------------------------
  # 5-6  Article push (推文) — service account & subscription account
  # ------------------------------------------------------------------
  art <- list(
    # Service account
    list(c("QMvFWHvSTvNTNvGZ"),                "服务号推文-NTN-周五广州",          "2024-01-26", "推文-仅销售"),
    list(c("QMvFWHvSTvNTNvSH"),                "服务号推文-NTN-周五上海",          "2024-01-26", "推文-仅销售"),
    list(c("QMvFWHvSTvNTNvBJ"),                "服务号推文-NTN-周五北京",          "2024-01-26", "推文-仅销售"),
    list(c("FFHvWTvZWvMAvSZvA"),               "服务号推文-周五深圳",              "2024-01-19", "推文-仅销售"),
    list(c("QMvFWHvEvEAvS"),                   "服务号推文-黑胶公告",              "2023-12-16", "推文-含品宣"),
    list(c("QMvFWHvEvDBvS"),                   "服务号推文-MA北京收官",            "2023-12-16", "推文-含品宣"),
    list(c("QMvFWHvCvEBvR"),                   "服务号推文-MOR剧照上新",           "2023-12-08", "推文-含品宣"),
    list(c("QMvFWHvCvFAvP"),                   "服务号推文-曲目公开",              "2023-11-22", "推文-含品宣"),
    list(c("QMvFWHvCvCAvP"),                   "服务号推文-歌词壁纸",              "2023-11-22", "推文-含品宣"),
    list(c("QMvFWHvCvDAvN"),                   "服务号推文-志愿者招募",            "2023-11-16", "推文-仅销售"),
    list(c("QMvFWHvDvAAvMAvBJ", "QMvFWHvDvADvMAvALL", "QMvFWHvDvABvMAvSH"),
                                               "服务号推文-MA三轮全票档",          "2023-11-16", "推文-开票"),
    list(c("QMvFWHvCvAAvM", "QMvFWHvCvABvM", "QMvFWHvCvACvM"),
                                               "服务号推文-MA三轮早鸟",            "2023-11-08", "推文-开票"),
    list(c("QMvFWHvCvCAvMAvALL"),              "服务号推文-MA中国巡演",            "2023-11-01", "推文-仅销售"),
    list(c("QMvFWHvCvAA", "QMvFWHvCvAC"),      "服务号推文-MOR二轮早鸟",          "2023-10-25", "推文-仅销售"),
    list(c("QMvFWHvCvBA", "QMvFWHvCvBE"),      "服务号推文-MA二轮全票档&工作坊",  "2023-10-25", "推文-仅销售"),
    list(c("QMvFWHvFvBB", "QMvFWHvFvBA"),      "服务号推文-长期次条MA&MOR",       "2023-10-14", "推文-仅销售"),
    list(c("QMvFWHvFvAA", "QMvFWHvFvAB", "QMvFWHvFvAC"),
                                               "服务号推文-MA二轮早鸟开票",        "2023-10-14", "推文-仅销售"),
    # Subscription account
    list(c("GZHTWvZYvTTvNTNvA"),               "推文-头条-MA回顾-NTN总",          "2024-01-29", "推文-含品宣"),
    list(c("GZHTWvZSvTTvMAvSZvC"),             "推文-周三头条深圳",               "2024-01-24", "推文-含品宣"),
    list(c("GZHTWvZEvTTvNTNvSZvA", "GZHTWvZEvTTvNTNvBJvA",
           "GZHTWvZEvTTvNTNvGZvA", "GZHTWvZEvTTvNTNvSHvA"),
                                               "推文-周二头条-总",                "2024-01-23", "推文-开票"),
    list(c("GZHTWvZYvTTvMAvSZvA"),             "推文-周一头条深圳",               "2024-01-22", "推文-含品宣"),
    list(c("QMvGZHvAvAAvX"),                   "推文-周一头条上海",               "2024-01-15", "推文-含品宣"),
    list(c("QMvGZHvAvAAvV"),                   "推文-新年启程",                   "2024-01-01", "推文-含品宣"),
    list(c("QMvGZHvCvABvW"),                   "推文-MA周四惊喜",                 "2024-01-10", "推文-含品宣"),
    list(c("QMvGZHvBvAAvW"),                   "推文-MA治愈暴躁",                 "2024-01-09", "推文-含品宣"),
    list(c("QMvGZHvDvAAvV"),                   "推文-MA给人以力量",               "2024-01-04", "推文-含品宣"),
    list(c("QMvGZHvCvABvV"),                   "推文-MA推荐理由",                 "2024-01-03", "推文-含品宣"),
    list(c("QMvGZHvBvAAvV"),                   "推文-MA首演周",                   "2024-01-02", "推文-含品宣"),
    list(c("QMvGZHvEvABvU", "QMvGZHvEvAAvU", "QMvGZHvEvACvU"),
                                               "推文-MA上海首演",                 "2023-12-29", "推文-含品宣"),
    list(c("QMvGZHvCvAAvU"),                   "推文-NTN定档",                    "2023-12-27", "推文-含品宣"),
    list(c("QMvGZHvCvBAvU"),                   "推文-MA上海倒计时2天",            "2023-12-27", "推文-仅销售"),
    list(c("QMvGZHvBvAAvU"),                   "推文-MA上海新开",                 "2023-12-26", "推文-开票"),
    list(c("QMvGZHvDvAAvT"),                   "推文-MA翻译幕后",                 "2023-12-21", "推文-含品宣"),
    list(c("QMvGZHvBvAAvT"),                   "推文-MA舞台技术工作坊",           "2023-12-19", "推文-含品宣"),
    list(c("QMvGZHvAvAAvS", "QMvGZHvAvABvS"),  "推文-MA北京收官",                 "2023-12-11", "推文-含品宣"),
    list(c("QMvGZHvEvABvR"),                   "推文-MA四轮上海早鸟开票",         "2023-12-08", "推文-开票"),
    list(c("QMvGZHvCvBBvR"),                   "推文-会昌戏剧节",                 "2023-12-06", "推文-仅销售"),
    list(c("QMvGZHvDvBCvR"),                   "推文-MA最后3天",                  "2023-12-07", "推文-仅销售"),
    list(c("QMvGZHvCvACvR"),                   "推文-MA最后4天",                  "2023-12-06", "推文-仅销售"),
    list(c("QMvGZHvBvBAvR"),                   "推文-MA最后5天",                  "2023-12-05", "推文-仅销售"),
    list(c("QMvGZHvAvBAvR"),                   "推文-MOR剧照上新",                "2023-12-04", "推文-含品宣"),
    list(c("QMvGZHvAvABvR"),                   "推文-MA最后一周",                 "2023-12-04", "推文-仅销售"),
    list(c("QMvGZHvEvAAvQ"),                   "推文-MOR上海首演",                "2023-12-01", "推文-含品宣"),
    list(c("QMvGZHvAvAAvQ", "QMvGZHvAvABvQ"),  "推文-MA票房",                     "2023-11-27", "推文-仅销售"),
    list(c("QMvGZHvEvABvP"),                   "推文-MOR深圳首演",                "2023-11-24", "推文-含品宣"),
    list(c("QMvGZHvDvBAvP"),                   "推文-MOR深圳观剧指南",            "2023-11-23", "推文-含品宣"),
    list(c("QMvGZHvDvAAvP"),                   "推文-工作坊回顾",                 "2023-11-23", "推文-含品宣"),
    list(c("QMvGZHvCvAAvP", "QMvGZHvCvABvP"),  "推文-歌词壁纸",                   "2023-11-22", "推文-含品宣"),
    list(c("QMvGZHvBvAAvP"),                   "推文-满江采访",                   "2023-11-21", "推文-含品宣"),
    list(c("QMvGZHvAvAAvP", "QMvGZHvAvABvP", "QMvGZHvAvACvP"),
                                               "推文-MA首演回顾",                 "2023-11-20", "推文-含品宣"),
    list(c("QMvGZHvEvAAvN"),                   "推文-MA装台视频",                 "2023-11-17", "推文-含品宣"),
    list(c("QMvGZHvDvBAvN"),                   "推文-MA观剧指南",                 "2023-11-16", "推文-含品宣"),
    list(c("QMvGZHvDvAAvO", "QMvGZHvDvABvO", "QMvGZHvDvACvO"),
                                               "推文-MA首演倒计时1天",            "2023-11-16", "推文-仅销售"),
    list(c("QMvGZHvCvAAvMAvBJva", "QMvGZHvCvABvMAvSH", "QMvGZHvCvACvMAvSZva"),
                                               "推文-MA卡司官宣",                 "2023-11-15", "推文-含品宣"),
    list(c("QMvGZHvBvABvN", "QMvGZHvBvAAvN"),  "推文-MA三轮全票档开票",           "2023-11-14", "推文-开票"),
    list(c("QMvGZHvAvABvN"),                   "推文-MOR定妆照",                  "2023-11-13", "推文-含品宣"),
    list(c("QMvGZHvAvAAvM", "QMvGZHvCvAAvMAvBJ",
           "QMvGZHvCvACvMAvSZ", "QMvGZHvABvMAvSH"),
                                               "推文-MA三轮早鸟开票",             "2023-11-08", "推文-开票"),
    list(c("QMvGZHvBvAAvM", "QMvGZHvBvABvM"),  "推文-MA三轮早鸟预告",             "2023-11-07", "推文-含品宣"),
    list(c("QMvGZHvAvABvM", "QMvGZHvAvACvM"),  "推文-预演回顾",                   "2023-11-06", "推文-含品宣"),
    list(c("QMvGZHvEvAAvL"),                   "推文-MOR装台视频",                "2023-11-03", "推文-含品宣"),
    list(c("QMvXCPvALL", "QMvGZHvDvABvL"),     "推文-MOR首演倒计时1天",           "2023-11-02", "推文-仅销售"),
    list(c("QMvGZHvCvBAvL"),                   "推文-MOR首演倒计时2天",           "2023-11-01", "推文-仅销售"),
    list(c("QMvGZHvBvBAvL"),                   "推文-MOR首演倒计时3天",           "2023-10-31", "推文-仅销售"),
    list(c("QMvGZHvBvABvL", "QMvGZHvBvAAvL"),  "推文-Mary专访",                   "2023-10-31", "推文-含品宣"),
    list(c("QMvGZHvEvABvK", "QMvGZHvEvAAvK"),  "推文-MOR二轮全票档开票",          "2023-10-27", "推文-开票"),
    list(c("QMvGZHvDvABvK", "QMvGZHvDvAAvK"),  "推文-《致埃文·汉森》演员招募",   "2023-10-26", "推文-含品宣"),
    list(c("QMvGZHvCvBAvK"),                   "推文-MOR二轮早鸟倒计时1天",       "2023-10-25", "推文-仅销售"),
    list(c("QMvGZHvBvBAvK"),                   "推文-MA工作坊上线",               "2023-10-24", "推文-仅销售"),
    list(c("QMvGZHvEvMORvSH", "QMvGZHvEvABvMORALL", "QMvGZHvEvAAvMORALL"),
                                               "推文-MOR二轮早鸟开票",            "2023-10-20", "推文-开票"),
    list(c("QMvGZHvDvBavMORALL"),              "推文-MOR二轮早鸟开票预告",        "2023-10-19", "推文-仅销售"),
    list(c("QMvGZHvCvBBvJ", "QMvGZHvCvBCvJ", "QMvGZHvCvBAvJ"),
                                               "推文-MA二轮全票档开票",           "2023-10-18", "推文-开票"),
    list(c("QMvGZHvBvBAvJ"),                   "推文-MA二轮全票档预告",           "2023-10-17", "推文-仅销售"),
    list(c("QMvGZHvBvAAvJ"),                   "推文-导演专访",                   "2023-10-17", "推文-含品宣"),
    list(c("QMvGZHvAvBAvJ"),                   "推文-MA二轮早鸟结束倒计时1天",    "2023-10-16", "推文-仅销售"),
    list(c("QMvGZHvBvBB"),                     "推文-告别铁窗泪",                 "2023-10-11", "推文-含品宣"),
    list(c("QMvGZHvCvABvI", "QMvGZHvCvACvI", "QMvGZHvCvADvI"),
                                               "推文-MA二轮早鸟开票",             "2023-10-11", "推文-开票"),
    list(c("QMvGZHvBvAAvI"),                   "推文-MA二轮早鸟倒计时1天",        "2023-10-10", "推文-仅销售"),
    list(c("QMvGZHvAvAAvI"),                   "推文-周一冷笑话",                 "2023-10-09", "推文-含品宣"),
    list(c("QMvGZHvDvBAvG"),                   "推文-抽奖开奖",                   "2023-09-28", "推文-含品宣"),
    list(c("QMvGZHvDvAAvG"),                   "推文-演员招募",                   "2023-09-28", "推文-含品宣"),
    list(c("QMvGZHvCvAAvD"),                   "推文-假日片单",                   "2023-09-27", "推文-含品宣"),
    list(c("QMvGZHvAvAAvA", "QMvGZHvAvABvA", "QMvGZHvAvACvA", "QMvGZHvAvADvA"),
                                               "推文-叉腰小女孩有多牛",           "2023-09-25", "推文-含品宣"),
    list(c("QMvGZHvDvBAvA"),                   "推文-宫廷乐手招募",               "2023-09-21", "推文-含品宣"),
    list(c("QMvGZHvEvAAa", "QMvGZHvEvABb", "QMvGZHvEvACc", "QMvGZHvEvADd"),
                                               "推文-MArepo合集",                 "2023-09-22", "推文-含品宣"),
    list(c("QMvGZHvDvAAvE"),                   "推文-MOR交流会名单",              "2023-09-21", "推文-含品宣"),
    list(c("QMvGZHvCvAAvC", "QMvGZHvCvABvB", "QMvGZHvCvACvB", "QMvGZHvCvADvB"),
                                               "推文-MA全票档",                   "2023-09-20", "推文-开票"),
    list(c("QMvGZHvAvAA"),                     "推文-MA早鸟倒计时1天",            "2023-09-18", "推文-仅销售"),
    list(c("QMvGZHvFvAAvA"),                   "推文-MA早鸟倒计时3天",            "2023-09-16", "推文-仅销售"),
    list(c("QMvGZHvEvAAvC"),                   "推文-MOR交流会招募",              "2023-09-15", "推文-含品宣"),
    list(c("QMvGZHvDvAAvD"),                   "推文-MOR排练照",                  "2023-09-14", "推文-含品宣"),
    list(c("QMvGZHvCvAAvB", "QMvGZHvCvABvA", "QMvGZHvCvACvA", "QMvGZHvCvADvA"),
                                               "推文-MA早鸟开票",                 "2023-09-13", "推文-开票"),
    list(c("QMvGZHvAZ"),                       "推文-MA开票预告",                 "2023-09-11", "推文-仅销售"),
    list(c("QMvGZHvDvAA", "QMvGZHvDvAB", "QMvGZHvDvAZ"),
                                               "推文-MOR幕后故事",                "2023-08-24", "推文-含品宣"),
    list(c("QMvGZHvEvAAvC"),                   "推文-MOR选角",                    "2023-08-25", "推文-含品宣"),
    list(c("QMvGZHvEvAAvA", "QMvGZHvEvABvA", "QMvGZHvEvAC", "QMvGZHvEvAD", "QMvGZHvEvAE"),
                                               "推文-龚勋专访",                   "2023-09-01", "推文-含品宣"),
    list(c("QMvGZHvDvAAvB", "QMvGZHvDvABvA", "QMvGZHvDvAC", "QMvGZHvDvAD", "QMvGZHvDvAE"),
                                               "推文-布料视频",                   "2023-08-31", "推文-含品宣"),
    list(c("QMvGZHvAvE", "QMvGZHvAvA"),        "推文-MOR建组",                    "2023-09-04", "推文-含品宣"),
    list(c("QMvGZHvCvBAvA", "QMvGZHvCvBBvA"),  "推文-MOR建组前序",                "2023-09-05", "推文-含品宣"),
    list(c("QMvGZHvCvBAvB"),                   "推文-MOR上海深圳开票预告",        "2023-09-05", "推文-仅销售"),
    list(c("QMvGZHvCvAAvA", "QMvGZHvCvAB", "QMvGZHvCvAC", "QMvGZHvCvAD",
           "QMvGZHvCvAE", "QMvGZHvCvAF", "QMvGZHvCvAG"),
                                               "推文-MOR上海深圳全票档开票",      "2023-09-06", "推文-开票"),
    list(c("QM"),                              "推文-排练日记EP.1",               "2023-09-07", "推文-含品宣"),
    list(c("QMvGZHvEvAAvB", "QMvGZHvEvABvB", "QMvGZHvEvACvA", "QMvGZHvEvADvA"),
                                               "推文-灯光专访",                   "2023-09-08", "推文-含品宣"),
    list(c("QMvGZHvCTvBJYY", "QMvGZHvCTvSHZS", "QMvGZHvCTvSZZS", "QMvGZHvCTvBJZS"),
                                               "推文-MOR长期次条",                "2023-09-11", "推文-仅销售"),
    list(c("QMvGZHvCTvNTNvZT"),               "推文-NTN长期次条",                "2024-01-24", "推文-仅销售"),
    list(c("QMvGZHvCTvMOR", "QMvGZHvCTvMA", "QMvGZHvBvBA",
           "QMvGZHvCvBBvMORvALL", "QMvGZHvCvBAvMAvALL", "QMvGZHvDvCAvO"),
                                               "推文-长期次条MA&MOR",             "2023-09-14", "推文-仅销售"),
    list(c("QMvGZHvFvAA", "QMvGZHvFvAB", "QMvGZHvFvAC", "QMvGZHvFvAD"),
                                               "推文-MOR天桥全票档开票",          "2023-09-02", "推文-开票")
  )
  for (a in art) set3(is.element(text, a[[1]]), a[[2]], a[[3]], a[[4]])

  # Early article batches (time-window disambiguation)
  set3(text == "gongzhonghaozhanshi" &
         time < as.POSIXct("2023-08-21 18:55:00", tz = "UTC"),
       "推文-MOR早鸟开票", "2023-08-17", "推文-开票")
  set3(is.element(text, c("gongzhonghaozhanshi", "weixinyueduyuanwen")) &
         time >= as.POSIXct("2023-08-21 18:55:00", tz = "UTC") &
         time <  as.POSIXct("2023-08-22 18:55:00", tz = "UTC"),
       "推文-亲笔信", "2023-08-21", "推文-含品宣")
  set3(text == "daojishixiaotuiwen" &
         time >= as.POSIXct("2023-08-21 18:55:00", tz = "UTC") &
         time <  as.POSIXct("2023-08-22 18:55:00", tz = "UTC"),
       "推文-MOR早鸟倒计时2天", "2023-08-21", "推文-仅销售")
  set3(is.element(text, c("gongzhonghaozhanshi", "weixinyueduyuanwen")) &
         time >= as.POSIXct("2023-08-22 18:55:00", tz = "UTC") &
         time <  as.POSIXct("2023-08-23 15:00:00", tz = "UTC"),
       "推文-MOR主创官宣上", "2023-08-22", "推文-含品宣")
  set3(text == "daojishixiaotuiwen" & time >= as.POSIXct("2023-08-22 18:55:00", tz = "UTC"),
       "推文-MOR早鸟倒计时1天", "2023-08-22", "推文-仅销售")
  set3(is.element(text, c("gongzhonghaozhanshi", "weixinyueduyuanwen")) &
         time >= as.POSIXct("2023-08-23 15:00:00", tz = "UTC"),
       "推文-MOR全票档开票", "2023-08-23", "推文-开票")
  set3(is.element(text, c("QMvGZHvGvAA", "QMvGZHvGvAB", "QMvGZHvGvAC", "QMvGZHvGvAD",
                           "QMvGZHvGvAE", "QMvGZHvGvAF", "QMvGZHvGvAG", "QMvGZHvGvAH",
                           "QMvGZHvGvAZ")) &
         time >= as.POSIXct("2023-08-27", tz = "UTC"),
       "推文-MOR正式场早鸟开票", "2023-08-27", "推文-开票")
  set3(text == "QMvGZHvFvBA",
       "推文-MOR正式场早鸟预告", "2023-08-27", "推文-开票")
  set3(text == "QMvGZHvDove",
       "推文-Dove视频", "2023-08-29", "推文-含品宣")
  set3(is.element(text, c("QMvGZHvCvAA", "QMvGZHvDJSvC")),
       "推文-MOR正式场早鸟倒计时3天", "2023-08-30", "推文-仅销售")
  set3(text == "QMvGZHvDvBA",
       "推文-MOR正式场早鸟倒计时2天", "2023-08-31", "推文-仅销售")
  set3(text == "QMvGZHvDJSvA",
       "推文-MOR正式场早鸟倒计时1天", "2023-09-01", "推文-仅销售")
  set3(is.element(text, c("QMvGZHvEvAAvB", "QMvGZHvEvABvB", "QMvGZHvEvACvA", "QMvGZHvEvADvA")),
       "推文-灯光专访", "2023-09-08", "推文-含品宣")

  # ------------------------------------------------------------------
  # 5-7  External partner articles
  # ------------------------------------------------------------------
  set1(is.element(text, c("XCvEQJCvTW", "XCvEQJCvD")), "二七推文", "推文-外部")

  # ------------------------------------------------------------------
  # 5-8  Weibo
  # ------------------------------------------------------------------
  set3(text == "QMvWBvAvAAvN",                         "微博-定妆照",     "2023-11-13", "微博")
  set3(text == "QMvWBvMORvSZ",                         "微博-预演结束",   "2023-11-05", "微博")
  set3(text == "QMvWBvBvAAvL",                         "微博-预演倒计时3天", "2023-10-31", "微博")
  set3(text == "QMvWBvBvA",                            "微博-排练照",     "2023-10-17", "微博")
  set1(is.element(text, c("fazhaweibo", "QMvWBvB")),   "微博",            "微博")
  set3(text == "QMvWBvD",                              "微博",            "2023-09-21", "微博")

  # ------------------------------------------------------------------
  # 5-9  Enterprise WeChat group (企微群)
  # ------------------------------------------------------------------
  set3(text == "QMvQWQvMAvEA",     "企微群-MA五轮",        "2023-12-26", "企微群")
  set3(text == "QMvQWQvMAvC",      "企微群-MA三轮早鸟",    "2023-11-08", "企微群")
  set3(text == "QMvQWQvMORvC",     "企微群-MOR二轮全票档", "2023-10-27", "企微群")
  set3(text == "QMvXQPXQWQvMAvB",  "培训企微群-MA二轮全票档", "2023-10-18", "企微群")
  set3(text == "QMvQWQvCvMAvA",    "企微群-MA二轮早鸟",    "2023-10-11", "企微群")
  set3(text == "QMvQWQvCvMAvQBD",  "企微群-MA全票档",      "2023-09-20", "企微群")
  set3(text == "QMvQWQvCvMA",      "企微群-MA早鸟",        "2023-09-13", "企微群")
  set1(text == "qiweiqun",         "企微群",               "企微群")
  set3(text == "QMvQWQvGvALL",     "企微群",               "2023-08-27", "企微群")

  # ------------------------------------------------------------------
  # 5-10  Enterprise WeChat moments (企微朋友圈)
  # ------------------------------------------------------------------
  set3(text == "QMvPYQvNTNvALLvAa", "朋友圈-NTN第一轮早鸟",      "2024-01-23", "企微朋友圈")
  set3(text == "QMvPYQvMAvEA",      "企微朋友圈-MA五轮",          "2023-12-26", "企微朋友圈")
  set3(text == "QMvPYQvMAvC",       "企微朋友圈-MA三轮早鸟",      "2023-11-08", "企微朋友圈")
  set3(text == "QMvPXvPYQvMAvC",    "培训-企微朋友圈-MA三轮早鸟", "2023-11-08", "企微朋友圈")
  set3(text == "QMvPYQvMAvB",       "企微朋友圈-MA二轮全票档",    "2023-10-18", "企微朋友圈")
  set3(text == "QMvPYQvCvMAvA",     "企微朋友圈-MA二轮早鸟",      "2023-10-11", "企微朋友圈")
  set3(text == "QMvPYQvCvMAvQBD",   "企微朋友圈-MA全票档",        "2023-09-20", "企微朋友圈")
  set3(text == "QMvPYQvCvMA",       "企微朋友圈-MA早鸟",          "2023-09-13", "企微朋友圈")
  set1(text == "qiweipengyouquan",  "企微朋友圈",                  "企微朋友圈")
  set3(text == "QMvPYQvGvALL",      "企微朋友圈",                  "2023-08-27", "企微朋友圈")

  # ------------------------------------------------------------------
  # 5-11  Official-account display slots (公众号展示)
  # ------------------------------------------------------------------
  set1(is.element(text, "QMvGZHvZSvMAvSZ"),       "订阅号展示-MA深圳",  "公众号展示")
  set1(is.element(text, "QMvGZHvZSvNTNvALL"),     "订阅号展示-NTN",     "公众号展示")
  set1(is.element(text, "QMvGZHZSvMAvALL"),       "订阅号展示-MA",      "公众号展示")
  set1(is.element(text, "QMvGZHZSvMORvALL"),      "订阅号展示-MOR",     "公众号展示")
  set1(is.element(text, "QMvFWHvMA"),              "服务号展示-MA",      "公众号展示")
  set1(is.element(text, "QMvFWHvMOR"),             "服务号展示-MOR",     "公众号展示")
  set1(text == "QMvGZHvZSvSZ",                    "订阅号展示-深圳",    "公众号展示")
  set1(text == "QMvGZHvZSvBJZSC",                 "订阅号展示-北京",    "公众号展示")
  set1(text == "QMvGZHvZSvSH",                    "订阅号展示-上海",    "公众号展示")

  # ------------------------------------------------------------------
  # 5-12  Paid advertising (投放)
  # ------------------------------------------------------------------
  ads <- list(
    "TFvGDTvMAvSZvSP"       = "投放-广点通-深圳",
    "TFvXHSvMAvSZ"          = "投放-小红书-深圳",
    "TFvWTJYvMAvALL"        = "投放-外滩教育",
    "TFvGDTvDTvMAvBJ"       = "投放-广点通-北京",
    "TFvGDTvLGGvMAvBJ"      = "投放-广点通-北京",
    "TFvGDTvDTvMORvSH"      = "投放-广点通-上海",
    "TFvDDPDQXJvMAvALL"     = "投放-帝都胖豆求学记",
    "TFvMLMAFXvMAvALL"      = "投放-米粒妈爱分享",
    "FXvDSPHQLXvMORvALL"    = "投放-带上屁孩去旅行",
    "TFvDSFCHZvMAvALL"      = "投放-朵师傅闯黄庄",
    "TFvMIMPDvMAvALL"       = "投放-米粒妈频道",
    "TFvYJKDYvMAvALLvCR"    = "投放-伊姐看电影",
    "TFvBYXXTvMAvCHERRY"    = "投放-博雅小学堂",
    "TFvQTMMQWYvMAvALLvCR"  = "投放-晴天妈妈千万亿",
    "TFvNNMAFXvMAvALLvCR"   = "投放-暖暖妈爱分享",
    "TFvGBDZLDvMAvALLvCR"   = "投放-果爸的自留地",
    "TFvTSMMSCLvMAvALLvCR"  = "投放-童书妈妈三川玲",
    "TFvTXSYvMAvALLvCR"     = "投放-童行书院",
    "TFvGDTvSPvMAvBJ"       = "投放-广点通-北京",
    "TFvGDTvSPvMAvSH"       = "投放-广点通-上海",
    "TFvGDTvDTvMAvSH"       = "投放-广点通-上海",
    "TFvGDTvSPvMAvSHvZZ"    = "投放-广点通-上海",
    "TFvGDTvMAvSHvN"        = "投放-广点通-上海",
    "TFvLXSvMAvSHvCG"       = "投放-蓝橡树-上海",
    "TFvLXSvMAvSHvZCX"      = "投放-蓝橡树-上海",
    "TFvLXSvMAvSHvZZ"       = "投放-蓝橡树-上海",
    "TFvGDideavMAvSHvZZ"    = "投放-姑的idea-上海",
    "TFvDSXYvMAvSHvCG"      = "投放-大俗小雅-上海",
    "TFvLXQZDvMAvSHvYJ"     = "投放-留学全知道-上海",
    "NXHSvRLYJACGvMAvBJ"    = "小红书-瑞拉一家爱唱歌",
    "TFvYJKDYvMORvBJTQ"     = "投放-伊姐看电影-北京",
    "TFvXHSvMAvSH"          = "投放-小红书-上海"
  )
  for (code in names(ads)) set1(text == code, ads[[code]], "投放")

  # ------------------------------------------------------------------
  # 5-13  Miscellaneous
  # ------------------------------------------------------------------
  set1(text == "QMvXQvAUTO",           "小七自动回复",         "小七自动回复")
  set1(text == "QMvMTJBJMJSvMORvALL",  "媒体嘉宾剧目介绍文字", "剧目介绍文字")
  set1(text == "QMvPYQvDKvALL",        "邓柯-朋友圈",          "乐评人邓柯")
  set1(text == "QMvMTvMAvDXKBJ",       "大戏看北京文艺资讯",   "媒体")
  set1(text == "QMvMTvSHBUYBUYvMA",    "上海buybuy",           "媒体")
  set1(text == "QMvSP",                "视频",                 "视频")
  set1(text == "PXvXYNGvMAvBJvTSvN",   "培训-校园内购-北京",   "大客户")
  set1(text == "QMvMAZYvALl",          "周边-MA折页",          "周边")
  set1(text == "CSvMAvBJvBJ",          "初爽-教育半价",        "员工")
  set1(text == "HCYvMAvBJvBJ",         "黄晨颖-北京-半价",     "员工")
  set1(text == "ZYXvMAvBJvBJ",         "张燕雪-北京-半价",     "员工")
  set1(text == "PXvHJvCSvMAvBJvZZ",    "培训-初爽-汇佳-北京",  "员工")
  set1(text == "PXvCScHDvMAvBJvZZ",    "培训-初爽-赫德-北京",  "员工")
  set1(text == "SQvMAvSHvZZvCd",       "社群-周中-上海",       "社群")
  set1(text == "XCvSQ",                "宣传-社群",            "社群")
  set1(text == "XCvSQvPX",             "宣传-社群-培训",       "社群")
  set1(text == "SQvDDDvMAvSZvA",       "点对点-社群-深圳",     "社群")
  set1(text == "SQvDDDvMAvSHvA",       "点对点-社群-上海",     "社群")
  set1(text == "QMvXCvKBLLZHvALL",     "口碑流量转化",         "口碑流量转化")
  set1(text == "QMvTIMSvPJvMORvALL",   "市场-TIMS票夹",        "市场")
  set1(text == "QMvMAvXCGPvMORvBJ",    "MA观众购MOR优惠",      "优惠")
  set1(text == "QMvSQvNTNvALLvAa",     "社群-NTN-全",          "社群")
  set1(text == "SQvNTNvZWvSZvA",       "社群-NTN早鸟-深圳",    "社群")
  set1(is.element(text, c("SQvMAvSHvZZvF", "SQvMAvSHvZZvE", "SQvMAvSHvZZvG",
                           "SQvMAvSHvZZvB", "SQvMAvSHvZZvC", "SQvMAvSHvZZvD",
                           "SQvMAvSHvZZvEb", "SQvMAvSHvZZvFb", "SQvMAvSHvZZvGb",
                           "SQvMAvSHvZZvCc", "SQvMAvSHvZZvDc")),
       "社群-MA-上海", "社群")

  # Warn about any unresolved codes
  if (any(ans_m == "其他")) {
    print(sort(unique(text[(which(ans_m == "其他") + 2) %/% 3])))
    warning("mark_from: unresolved referral code(s)")
  }
  t(ans_m)
}

# =============================================================================
# 6. Campaign reach / cost statistics (mark_people_num)
#    Returns a 2-column matrix: [1] reach (people/messages), [2] cost (CNY)
# =============================================================================

mark_people_num <- function(name, date) {
  # SMS unit cost: 0.035 CNY per message
  l     <- length(name)
  ans_m <- matrix(rep("", 2 * l), 2, l)

  # WeChat 1-to-1 reach (number of recipients)
  reach <- list(
    list("小七点对点",                    "2023-08-17", "5886"),
    list("小七点对点",                    "2023-08-18", "6442"),
    list("小七点对点",                    "2023-08-21", "10388"),
    list("小七点对点",                    "2023-08-23", "11435"),
    list("小七点对点-深圳",               "2023-08-27", "4583"),
    list("小七点对点-深圳-全票档",        "2023-09-06", "15502"),
    list("小七点对点-上海",               "2023-08-27", "3561"),
    list("小七点对点-上海-全票档",        "2023-09-06", "1556"),
    list("小七点对点-北京-正式场",        "2023-08-27", "6655"),
    list("小七点对点-北京-正式场",        "2023-09-02", "19019"),
    list("小七点对点-MA早鸟-北京",        "2023-09-13", "9167"),
    list("小七点对点-MA早鸟-上海",        "2023-09-13", "3477"),
    list("小七点对点-MA早鸟-深圳",        "2023-09-13", "4546"),
    list("小七点对点-MA全票档-北京",      "2023-09-20", "20221"),
    list("小七点对点-MA全票档-上海",      "2023-09-20", "2691"),
    list("小七点对点-MA全票档-广深",      "2023-09-20", "392"),
    list("小七点对点-MA二轮早鸟-培训",   "2023-10-12", "1280"),
    list("小七点对点-MA二轮早鸟-上海",   "2023-10-11", "3403"),
    list("小七点对点-MA二轮早鸟-北京",   "2023-10-11", "19123"),
    list("小七点对点-MA二轮全票档-上海", "2023-10-18", "3381"),
    list("小七点对点-MA二轮全票档-北京", "2023-10-18", "5688"),
    list("小七点对点-MOR二轮早鸟",       "2023-10-20", "3358"),
    list("小七点对点-MA三轮早鸟",        "2023-11-08", "6583"),
    list("小七点对点-MA四轮早鸟",        "2023-12-08", "4098"),
    list("培训点对点-MA三轮早鸟提醒",    "2023-11-13", "6521"),
    list("小七点对点-MA三轮全票档",      "2023-11-14", "6491"),
    list("小七点对点-MOR二轮全票档",     "2023-10-27", "3292")
  )
  for (r in reach) ans_m[1, name == r[[1]] & date == r[[2]]] <- r[[3]]

  # SMS campaigns: [reach, cost]
  sms_stats <- list(
    list("短信",           "2023-08-17", "33189",  "2323.58"),
    list("短信",           "2023-08-21", "26627",  "2122.61"),
    list("短信",           "2023-08-24", "26477",  "1853.39"),
    list("短信-上海",      "2023-08-27", "29465",  "2062.55"),
    list("短信-北京",      "2023-08-27", "25122",  "1758.54"),
    list("短信-广深",      "2023-08-27", "8289",   "580.23"),
    list("短信-MA早鸟-金星会员",     "2023-09-13", "308",    "21.56"),
    list("短信-MA早鸟-银星会员",     "2023-09-13", "1160",   "81.2"),
    list("短信-MA早鸟-过期会员",     "2023-09-13", "4090",   "286.3"),
    list("短信-MA早鸟-上海",         "2023-09-13", "29524",  "2066.68"),
    list("短信-MA早鸟-北京",         "2023-09-13", "25606",  "1792.42"),
    list("短信-MA早鸟-广深",         "2023-09-13", "8342",   "583.94"),
    list("短信-MA全票档-会员",       "2023-09-20", "5557",   "388.99"),
    list("短信-MA全票档-广深",       "2023-09-20", "8347",   "584.29"),
    list("短信-MA全票档-北京",       "2023-09-20", "25547",  "1788.29"),
    list("短信-MA全票档-上海",       "2023-09-20", "29465",  "2062.55"),
    list("短信-MA二轮早鸟-北京",     "2023-10-11", "25405",  "1778.35"),
    list("短信-MA二轮早鸟-上海",     "2023-10-11", "29390",  "2057.3"),
    list("短信-MA二轮早鸟-金星会员", "2023-10-11", "447",    "46.935"),
    list("短信-MA二轮早鸟-银星会员", "2023-10-11", "1386",   "97.02"),
    list("短信-MA二轮早鸟-过期会员", "2023-10-11", "5963",   "417.41"),
    list("短信-MA二轮全票档-北京",   "2023-10-18", "25355",  "1774.85"),
    list("短信-MA二轮全票档-上海",   "2023-10-18", "29361",  "2055.27"),
    list("短信-MA二轮全票档-金星会员","2023-10-18","449",    "31.43"),
    list("短信-MA二轮全票档-银星会员","2023-10-18","1397",   "97.79"),
    list("短信-MA二轮全票档-过期会员","2023-10-18","5986",   "419.02"),
    list("短信-MOR二轮早鸟-上海",    "2023-10-20", "30429",  "2130.03"),
    list("短信-MOR二轮早鸟-金星会员","2023-10-20", "449",    "31.43"),
    list("短信-MOR二轮早鸟-银星会员","2023-10-20", "1394",   "97.58"),
    list("短信-MOR二轮早鸟-过期会员","2023-10-20", "5986",   "419.02"),
    list("短信-MOR二轮全票档-上海",  "2023-10-27", "30445",  "2131.15"),
    list("短信-MOR二轮全票档-过期会员","2023-10-27","5986",  "419.02"),
    list("短信-MOR二轮全票档-银星会员","2023-10-27","1391",  "97.37"),
    list("短信-MOR二轮全票档-金星会员","2023-10-27","449",   "40.41"),
    list("短信-MA三轮早鸟-金星会员", "2023-11-08", "463",    "32.41"),
    list("短信-MA三轮早鸟-银星会员", "2023-11-08", "1373",   "96.11"),
    list("短信-MA三轮早鸟-过期会员", "2023-11-08", "6015",   "421.05"),
    list("短信-MA三轮早鸟-北京",     "2023-11-08", "25357",  "1774.99"),
    list("短信-MA三轮早鸟结束提醒",  "2023-11-13", "27072",  "947.52"),
    list("短信-MA三轮全票档-过期会员","2023-11-14","6022",   "210.77"),
    list("短信-MA三轮全票档-银星会员","2023-11-14","1366",   "47.81"),
    list("短信-MA三轮全票档-金星会员","2023-11-14","460",    "16.1"),
    list("短信-MA三轮全票档-北京",   "2023-11-14", "25341",  "886.935")
  )
  for (s in sms_stats) {
    mask <- name == s[[1]] & date == s[[2]]
    ans_m[1, mask] <- s[[3]]
    ans_m[2, mask] <- s[[4]]
  }

  t(ans_m)
}

# =============================================================================
# 7. Distributor-system price / quantity parser (mark_fenxiao_price)
#    Returns a 2-column matrix: [1] face-value price, [2] ticket count
# =============================================================================

mark_fenxiao_price <- function(text) {
  l     <- length(text)
  ans_m <- matrix(rep(0, 2 * l), 2, l)

  # Auto-parse "原价NNN元" patterns
  pat <- regex("^原价([0-9]+)元$")
  ans_m[1, str_detect(text, pat)] <-
    as.numeric(str_match(text[str_detect(text, pat)], pat)[, 2])
  ans_m[2, str_detect(text, pat)] <- 1

  price_map <- list(
    `280`  = c("280（93折）"),
    `380`  = c("380"),
    `480`  = c("480", "480两张（第二张半价）", "480双张套票"),
    `580`  = c("580两张原价1160元", "580两张870元（第二张半价）", "580两张",
               "580两张（第二张半价）", "580两张买一赠一", "580"),
    `680`  = c("【套票】680两张原价1360元", "680两张原价1360元", "680",
               "680两张（第二张半价）", "680三张（第三张半价）", "680双张套票"),
    `780`  = c("780两张原价1560元", "780两张套票", "780两张1170元（第二张半价）",
               "780两张", "780两张（第二张半价）", "780两张买一赠一"),
    `880`  = c("880两张原价1760元", "880", "880三张套票原价2640元", "880三张套票",
               "880三张原价2640元", "880三张（第三张半价）"),
    `980`  = c("980两张套票原价1960元", "980两张1470元（第二张半价）", "980两张",
               "980两张（第二张半价）", "980两张买一赠一"),
    `1080` = c("1080", "1080三张套票", "1080三张原价3240元", "1080三张（第三张半价）")
  )
  for (price_str in names(price_map)) {
    ans_m[1, is.element(text, price_map[[price_str]])] <- as.numeric(price_str)
  }

  # Ticket-count multipliers
  n1 <- c("1080", "880", "680", "480", "380", "280（93折）", "580")
  n2 <- c("580两张原价1160元", "680两张原价1360元", "【套票】680两张原价1360元",
           "880两张原价1760元", "980两张套票原价1960元", "780两张原价1560元",
           "780两张套票", "780两张1170元（第二张半价）", "580两张870元（第二张半价）",
           "980两张1470元（第二张半价）", "480两张（第二张半价）", "680两张（第二张半价）",
           "580两张", "980两张", "780两张", "580两张（第二张半价）",
           "780两张（第二张半价）", "980两张（第二张半价）", "980两张买一赠一",
           "580两张买一赠一", "780两张买一赠一", "680双张套票", "480双张套票")
  n3 <- c("880三张套票原价2640元", "880三张套票", "1080三张套票",
           "880三张原价2640元", "1080三张原价3240元", "880三张（第三张半价）",
           "680三张（第三张半价）", "1080三张（第三张半价）")

  ans_m[2, is.element(text, n1)] <- 1
  ans_m[2, is.element(text, n2)] <- 2
  ans_m[2, is.element(text, n3)] <- 3
  ans_m[, text == "测试票种1"] <- c(0.01, 0.01)

  if (any(ans_m == 0)) {
    print(unique(text[(which(ans_m == 0) + 1) %/% 2]))
    warning("mark_fenxiao_price: unknown ticket type(s)")
  }
  t(ans_m)
}

# =============================================================================
# 8. Distributor-system account name lookup (mark_from_name)
# =============================================================================

mark_from_name <- function(text) {
  l   <- length(text)
  ans <- rep("未知", l)

  lookup <- c(
    "KHvJRDSvMORvBJTQ"        = "大客户-a",
    "KHvLSZYvMORvSHvTS"       = "大客户-b",
    "KHvHKLSSWSvMORvSHvTS"    = "大客户-c",
  )
  for (code in names(lookup)) ans[text == code] <- lookup[[code]]

  if (any(ans == "未知")) {
    print(unique(text[ans == "未知"]))
    warning("mark_from_name: unmatched distributor code(s)")
  }
  ans
}

# =============================================================================
# 9. Session / venue annotation (mark_project)
#    Maps (session_datetime, production_code) -> (project_name, venue_name)
# =============================================================================

mark_project <- function(date, jumu) {
  l     <- length(date)
  ans_m <- matrix(rep("", 2 * l), 2, l)

  project_map <- list(
    # MOR
    list("MOR", c("2023-11-03 19:30:00","2023-11-04 14:30:00","2023-11-05 14:30:00"),
         "北京预演", "二七剧场"),
    list("MOR", c("2023-11-24 20:00:00","2023-11-25 15:00:00","2023-11-25 20:00:00",
                  "2023-11-26 15:00:00","2023-11-26 20:00:00"),
         "深圳正式", "深圳保利剧院"),
    list("MOR", c("2023-12-01 19:30:00","2023-12-02 14:00:00","2023-12-02 19:30:00",
                  "2023-12-03 14:00:00","2023-12-03 19:30:00","2023-12-06 19:30:00",
                  "2023-12-07 19:30:00","2023-12-08 19:30:00","2023-12-09 14:00:00",
                  "2023-12-09 19:30:00","2023-12-10 14:00:00","2023-12-10 19:30:00"),
         "上海正式", "上海大剧院"),
    list("MOR", c("2023-12-15 19:30:00","2023-12-16 14:30:00","2023-12-16 19:30:00",
                  "2023-12-17 14:30:00","2023-12-17 19:30:00"),
         "北京正式", "北京天桥艺术中心"),
    # MA — Beijing
    list("MA", c("2023-11-17 19:30:00","2023-11-18 14:30:00","2023-11-18 19:30:00",
                 "2023-11-19 14:30:00","2023-11-19 19:30:00","2023-11-21 19:30:00",
                 "2023-11-22 19:30:00","2023-11-23 19:30:00","2023-11-24 19:30:00",
                 "2023-11-25 14:30:00","2023-11-25 19:30:00","2023-11-26 14:30:00",
                 "2023-11-26 19:30:00","2023-11-28 19:30:00","2023-11-29 19:30:00",
                 "2023-11-30 19:30:00","2023-12-01 19:30:00","2023-12-02 14:30:00",
                 "2023-12-02 19:30:00","2023-12-03 14:30:00","2023-12-03 19:30:00",
                 "2023-12-05 19:30:00","2023-12-06 19:30:00","2023-12-07 19:30:00",
                 "2023-12-08 19:30:00","2023-12-09 14:30:00","2023-12-09 19:30:00",
                 "2023-12-10 14:30:00","2023-12-10 19:30:00"),
         "北京MA", "二七剧场"),
    # MA — Shanghai
    list("MA", c("2023-12-29 19:30:00","2023-12-30 14:00:00","2023-12-30 19:30:00",
                 "2023-12-31 14:00:00","2023-12-31 19:30:00","2024-01-02 19:30:00",
                 "2024-01-03 19:30:00","2024-01-04 19:30:00","2024-01-05 19:30:00",
                 "2024-01-06 14:00:00","2024-01-06 19:30:00","2024-01-07 14:00:00",
                 "2024-01-07 19:30:00","2024-01-09 19:30:00","2024-01-10 19:30:00",
                 "2024-01-11 19:30:00","2024-01-12 19:30:00","2024-01-13 14:00:00",
                 "2024-01-13 19:30:00","2024-01-14 14:00:00","2024-01-14 19:30:00",
                 "2024-01-16 19:30:00","2024-01-17 19:30:00","2024-01-19 19:30:00",
                 "2024-01-20 14:00:00","2024-01-20 19:30:00","2024-01-21 14:00:00",
                 "2024-01-21 19:30:00"),
         "上海MA", "上海大剧院"),
    # MA — Shenzhen
    list("MA", c("2024-01-26 19:30:00","2024-01-27 14:30:00","2024-01-27 19:30:00",
                 "2024-01-28 14:30:00","2024-01-28 19:30:00"),
         "深圳MA", "深圳滨海")
  )
  for (pm in project_map) {
    mask <- jumu == pm[[1]] & is.element(date, pm[[2]])
    ans_m[1, mask] <- pm[[3]]
    ans_m[2, mask] <- pm[[4]]
  }
  # Test session
  ans_m[, date == "2023-09-01 10:00:00"] <- c("MOR-test", "MOR-test")

  if (any(ans_m == "")) {
    print(unique(date[ans_m[1,] == ""]))
    print(unique(jumu[ans_m[1,] == ""]))
    warning("mark_project: unrecognised session datetime/production")
  }
  t(ans_m)
}

# =============================================================================
# 10. Channel-type classifier for direct-sales orders (mark_qudao_zy)
# =============================================================================

mark_qudao_zy <- function(action_type) {
  dplyr::case_when(
    action_type == "大客户" ~ "大客户",
    action_type == "分销"   ~ "分销-系统",
    TRUE                    ~ "自营"
  )
}

# =============================================================================
# 11. Revenue multiplier for direct-sales (seated) tickets (mark_piaoti_zy)
#     Returns a character vector of multipliers (applied to face value).
# =============================================================================

mark_piaoti_zy <- function(channel_type, channel_name, production, face_price) {
  l   <- length(channel_name)
  ans <- rep("", l)

  # Default: full price for self-operated and bulk channels
  ans[channel_type %in% c("自营", "大客户")] <- "1"

  # Specific bulk-order discount (50 %)
  ans[channel_name == "大客户-施耐德"] <- "0.5"

  # Distributor partners with no rebate
  no_rebate <- c("带上屁孩去旅行", "北京剧中人", "一起旅课", "菁kids", "培训-嘉宝-EDM")
  ans[is.element(channel_name, no_rebate)] <- "1"
  ans[channel_name == "ikids" & production == "MOR"] <- "1"

  # Standard distributor rebate (10 % on MA tickets ≥780; full price below)/Anonymization
  fx_list <- c(
    "分销-a", "c", "b", "d", "e"
  )
  ans[is.element(channel_name, fx_list) & production == "MA" & face_price >= 780] <- "0.9"
  ans[is.element(channel_name, fx_list) & production == "MA" & face_price <  780] <- "1"
  ans[is.element(channel_name, fx_list) & production == "MOR"]                    <- "0.9"

  if (any(ans == "")) {
    print(unique(channel_name[ans == ""]))
    warning("mark_piaoti_zy: unknown channel revenue multiplier")
  }
  ans
}

# =============================================================================
# 12. Revenue multiplier + channel-type for distributor-system tickets
#     (mark_piaoti_fx)
#     Returns a 2-column matrix: [1] channel_type, [2] revenue_multiplier
# =============================================================================

mark_piaoti_fx <- function(channel_name, production, face_price) {
  l     <- length(channel_name)
  ans_m <- matrix(rep("", 2 * l), 2, l)

  fx_list <- c(
    "菁kids", "三叶草-深圳", "大觉观剧", "混在剧场", "帝国理工校友会",
    "一起旅课", "培训-嘉宝-EDM", "分销-童书妈妈"
  )
  dkh_list <- c(
    "大客户-a", "大客户-b", "大客户-c", "大客户-d"
  )
  zy_list <- c("自然销售", "分销系统-未知")

  ans_m[1, is.element(channel_name, fx_list)]  <- "分销-系统"
  ans_m[1, is.element(channel_name, dkh_list)] <- "大客户"
  ans_m[1, is.element(channel_name, zy_list)]  <- "自营"

  ans_m[2, is.element(channel_name, c(dkh_list, zy_list))]                            <- "1"
  ans_m[2, is.element(channel_name, fx_list) & production == "MA" & face_price <  780] <- "1"
  ans_m[2, is.element(channel_name, fx_list) & production == "MA" & face_price >= 780] <- "0.9"
  ans_m[2, is.element(channel_name, fx_list) & production == "MOR"]                    <- "0.9"
  ans_m[2, is.element(channel_name, c("一起旅课", "培训-嘉宝-EDM"))]                   <- "1"

  if (any(ans_m == "")) {
    print(unique(channel_name[(which(ans_m == "") + 1) %/% 2]))
    warning("mark_piaoti_fx: unknown channel info")
  }
  t(ans_m)
}

# =============================================================================
# 13. Shanghai Grand Theatre seat-inventory helpers
# =============================================================================

# Load the most recent (and previous) capacity reports from the venue
da_sh_djy_mor      <- readxl::read_xlsx("D:/实习工作-交接/日更数据/MOR数据日更/上海大剧院/莫扎特12-07.xlsx",  col_names = FALSE)
da_sh_djy_ma       <- readxl::read_xlsx("D:/实习工作-交接/日更数据/MA数据日更/上海大剧院/玛蒂尔达01-21.xlsx", col_names = FALSE)
da_sh_djy_ma_last  <- readxl::read_xlsx("D:/实习工作-交接/日更数据/MA数据日更/上海大剧院/玛蒂尔达01-18.xlsx", col_names = FALSE)

# Parse a venue report into a tidy sold/revenue table
parse_sh_djy <- function(raw, production_code) {
  find_tmp <- raw |> filter(...1 == "票价等级")
  col2     <- which(str_detect(as.matrix(find_tmp[3, ]), "正常票"))
  col3     <- which(str_detect(as.matrix(find_tmp[2, ]), "合计应收金额（元）"))
  pat      <- regex("^场次时间：([:print:]*)")

  valid_tiers <- c(
    "180(180.00)", "280(280.00)", "480(480.00)", "580(580.00)",
    "680(680.00)", "780(780.00)", "880(880.00)", "980(980.00)",
    "1080(1080.00)", "1280(1280.00)", "VIP(1180.00)", "VIP(1280.00)"
  )

  raw |>
    dplyr::select(c(1, col2, col3)) |>
    `colnames<-`(c("票价", "售票数", "收入")) |>
    mutate(场次时间 = str_match(票价, pat)[, 2]) |>
    filter(is.element(票价, valid_tiers) | !is.na(场次时间)) |>
    fill(场次时间) |>
    filter(!is.na(售票数)) |>
    mutate(
      票价     = as.numeric(str_match(票价, regex("[0-9]*[(]([:print:]*)[)]"))[, 2]),
      渠道类型 = "剧场",
      售票数   = as.numeric(售票数),
      收入     = as.numeric(收入),
      剧目     = production_code
    ) |>
    rename(场次 = 场次时间, 实收 = 收入, 张数 = 售票数)
}

da_sh_djy_kucun_ma_for_sell_summary      <- parse_sh_djy(da_sh_djy_ma,      "MA")
da_sh_djy_kucun_ma_for_sell_summary_last <- parse_sh_djy(da_sh_djy_ma_last, "MA")
da_sh_djy_kucun_mor_for_sell_summary     <- parse_sh_djy(da_sh_djy_mor,     "MOR")

# Query total tickets sold for given production / session / price tier(s)
sell_summmary <- function(data, jumu, changci, piaodang, show_income = FALSE) {
  l  <- max(length(jumu), length(changci), length(piaodang))
  if (length(jumu)    == 1) jumu    <- rep(jumu,    l)
  if (length(changci) == 1) changci <- rep(changci, l)
  if (length(piaodang)== 1) piaodang <- rep(piaodang, l)

  da <- bind_rows(
    data,
    da_sh_djy_kucun_ma_for_sell_summary,
    da_sh_djy_kucun_mor_for_sell_summary
  )

  ans <- matrix(numeric(2 * l), l, 2)
  for (i in seq_len(l)) {
    tmp <- da |>
      filter(is.element(剧目, jumu[i]),
             is.element(场次, changci[i]),
             is.element(票价, piaodang[i])) |>
      summarise(freq = sum(张数), income = sum(实收))
    ans[i, 1] <- tmp$freq
    ans[i, 2] <- tmp$income
  }
  if (show_income) ans else ans[, 1]
}

# Internal helper: look up seat inventory for a price-tier sequence
sell_kucun_help1 <- function(piaodang, kucun, seq) {
  if (length(piaodang) != length(kucun)) stop("sell_kucun_help1: length mismatch")
  if (any(!is.element(seq, piaodang))) {
    print(unique(seq[!is.element(seq, piaodang)]))
    stop("sell_kucun_help1: undefined price tier(s)")
  }
  vapply(seq, function(p) kucun[which(piaodang == p)], numeric(1))
}

# Look up total seat inventory by production / session / price tier
sell_kucun <- function(jumu, changci, piaodang) {
  l <- max(length(jumu), length(changci), length(piaodang))
  if (length(jumu)    == 1) jumu    <- rep(jumu,    l)
  if (length(changci) == 1) changci <- rep(changci, l)
  if (length(piaodang)== 1) piaodang <- rep(piaodang, l)
  ans <- numeric(l)

  # Session lists
  cc_ma_bj_wknd <- c("2023-11-17 19:30:00","2023-11-18 14:30:00","2023-11-18 19:30:00",
                     "2023-11-19 14:30:00","2023-11-19 19:30:00","2023-11-24 19:30:00",
                     "2023-11-25 14:30:00","2023-11-25 19:30:00","2023-11-26 14:30:00",
                     "2023-11-26 19:30:00","2023-12-01 19:30:00","2023-12-02 14:30:00",
                     "2023-12-02 19:30:00","2023-12-03 14:30:00","2023-12-03 19:30:00",
                     "2023-12-08 19:30:00","2023-12-09 14:30:00","2023-12-09 19:30:00",
                     "2023-12-10 14:30:00","2023-12-10 19:30:00")
  cc_ma_bj_week <- c("2023-11-21 19:30:00","2023-11-22 19:30:00","2023-11-23 19:30:00",
                     "2023-11-28 19:30:00","2023-11-29 19:30:00","2023-11-30 19:30:00",
                     "2023-12-05 19:30:00","2023-12-06 19:30:00","2023-12-07 19:30:00")
  cc_ma_sh_wknd <- c("2023-12-29 19:30:00","2023-12-30 14:00:00","2023-12-30 19:30:00",
                     "2023-12-31 14:00:00","2023-12-31 19:30:00","2024-01-05 19:30:00",
                     "2024-01-06 14:00:00","2024-01-06 19:30:00","2024-01-07 14:00:00",
                     "2024-01-07 19:30:00","2024-01-12 19:30:00","2024-01-13 14:00:00",
                     "2024-01-14 14:00:00","2024-01-14 19:30:00","2024-01-13 19:30:00",
                     "2024-01-19 19:30:00","2024-01-20 14:00:00","2024-01-20 19:30:00",
                     "2024-01-21 14:00:00","2024-01-21 19:30:00")
  cc_ma_sh_week <- c("2024-01-02 19:30:00","2024-01-03 19:30:00","2024-01-04 19:30:00",
                     "2024-01-09 19:30:00","2024-01-10 19:30:00","2024-01-11 19:30:00",
                     "2024-01-16 19:30:00","2024-01-17 19:30:00")
  cc_ma_sz_wknd <- c("2024-01-26 19:30:00","2024-01-27 14:30:00","2024-01-27 19:30:00",
                     "2024-01-28 14:30:00","2024-01-28 19:30:00")
  cc_mor_bjyy   <- c("2023-11-03 19:30:00","2023-11-04 14:30:00","2023-11-05 14:30:00")
  cc_mor_bj_wknd <- c("2023-12-15 19:30:00","2023-12-16 14:30:00",
                      "2023-12-16 19:30:00","2023-12-17 14:30:00")
  cc_mor_bj_week <- c("2023-12-17 19:30:00")
  cc_mor_sh_wknd <- c("2023-12-01 19:30:00","2023-12-02 14:00:00",
                      "2023-12-02 19:30:00","2023-12-03 14:00:00",
                      "2023-12-08 19:30:00","2023-12-09 14:00:00",
                      "2023-12-09 19:30:00","2023-12-10 14:00:00")
  cc_mor_sh_week <- c("2023-12-03 19:30:00","2023-12-06 19:30:00",
                      "2023-12-07 19:30:00","2023-12-10 19:30:00")
  cc_mor_sz_wknd <- c("2023-11-24 20:00:00","2023-11-25 15:00:00",
                      "2023-11-25 20:00:00","2023-11-26 15:00:00")
  cc_mor_sz_week <- c("2023-11-26 20:00:00")

  # Capacity vectors: seats per price tier
  kucun_ma_bj <- c(143, 355, 166, 118, 52, 254, 56)
  kucun_ma_sh <- c(105, 538, 244, 206, 128, 193, 135)
  kucun_ma_sz <- c(74,  557, 276, 137, 110, 75,  126)
  kucun_mor_bjyy  <- c(46,  221, 425, 66,  84,  290, 20)
  kucun_mor_bj    <- c(90,  215, 253, 435, 98,  343, 30)
  kucun_mor_sh    <- c(66,  178, 332, 357, 156, 370, 44)
  kucun_mor_sz    <- c(72,  210, 248, 316, 332, 252, 16)

  pd_ma_wknd      <- c(1280, 1080, 880, 680, 480, 280, 180)
  pd_ma_week      <- c(1180, 980,  780, 580, 480, 280, 180)
  pd_mor_yy       <- c(980,  880,  680, 580, 380, 280, 180)
  pd_mor_wknd_bj  <- c(1180, 1080, 880, 680, 480, 280, 180)
  pd_mor_wknd_sz  <- c(1080, 980,  780, 580, 480, 280, 180)
  pd_mor_week     <- c(980,  880,  680, 480, 380, 280, 180)

  lookup_sessions <- list(
    list(jumu == "MA" & is.element(changci, cc_ma_bj_wknd),  pd_ma_wknd,     kucun_ma_bj),
    list(jumu == "MA" & is.element(changci, cc_ma_bj_week),  pd_ma_week,     kucun_ma_bj),
    list(jumu == "MA" & is.element(changci, cc_ma_sh_wknd),  pd_ma_wknd,     kucun_ma_sh),
    list(jumu == "MA" & is.element(changci, cc_ma_sh_week),  pd_ma_week,     kucun_ma_sh),
    list(jumu == "MA" & is.element(changci, cc_ma_sz_wknd),  pd_ma_wknd,     kucun_ma_sz),
    list(jumu == "MOR" & is.element(changci, cc_mor_bjyy),   pd_mor_yy,      kucun_mor_bjyy),
    list(jumu == "MOR" & is.element(changci, cc_mor_bj_wknd),pd_mor_wknd_bj, kucun_mor_bj),
    list(jumu == "MOR" & is.element(changci, cc_mor_bj_week),pd_mor_week,    kucun_mor_bj),
    list(jumu == "MOR" & is.element(changci, cc_mor_sh_wknd),pd_mor_wknd_bj, kucun_mor_sh),
    list(jumu == "MOR" & is.element(changci, cc_mor_sh_week),pd_mor_week,    kucun_mor_sh),
    list(jumu == "MOR" & is.element(changci, cc_mor_sz_wknd),pd_mor_wknd_sz, kucun_mor_sz),
    list(jumu == "MOR" & is.element(changci, cc_mor_sz_week),pd_mor_week,    kucun_mor_sz)
  )
  for (ls in lookup_sessions) {
    mask <- ls[[1]]
    if (any(mask)) ans[mask] <- sell_kucun_help1(ls[[2]], ls[[3]], piaodang[mask])
  }

  if (any(ans == 0)) {
    print(unique(changci[ans == 0]))
    warning("sell_kucun: undefined session/production/price-tier combination")
  }
  ans
}

# =============================================================================
# 14. Bulk-order (团单) data integration
# =============================================================================

# Manually-entered bulk orders not captured in the ticketing system（Anonymization）
tuandan_raw <- c(
  "MOR","2023-12-03 14:00:00","2023-11-07 15:46:00","大客户-dxj",280,280,1,
  "MOR","2023-12-08 19:30:00","2023-11-07 15:46:00","大客户-dxj",280,280*13,13,
  "MOR","2023-12-10 19:30:00","2023-11-07 15:46:00","大客户-dxj",280,280*3,3,
  "MA","2024-01-06 14:00:00","2023-11-13 00:00:00","大客户-xfls",1080,896.4*4,4,
  "MA","2024-01-13 14:00:00","2023-11-13 00:00:00","大客户-xfls",1080,896.4*4,4,
  "MA","2024-01-27 14:30:00","2023-11-13 00:00:00","大客户-xfls",1280,1280*2,2,
  "MA","2024-01-27 14:30:00","2023-11-13 00:00:00","大客户-xfls",1080,896.4*2,2,
)

da_tuandan <- data.frame(
  matrix(tuandan_raw, length(tuandan_raw) / 7, 7, byrow = TRUE),
  stringsAsFactors = FALSE
) |>
  `colnames<-`(c("剧目", "场次", "下单时间", "渠道明细", "票价", "实收", "张数")) |>
  mutate(
    渠道类型 = "大客户",
    下单时间 = as.POSIXct(下单时间, tz = "UTC"),
    票价     = as.numeric(票价),
    实收     = as.numeric(实收),
    张数     = as.numeric(张数)
  )

# DingTalk approval-workflow bulk orders (three approval sheets)
da_tuandan_1 <- readxl::read_xlsx("D:/大客户团单-截至12_15.xlsx",
                                  sheet = "202311021126000122")
da_tuandan_2 <- readxl::read_xlsx("D:/大客户团单-截至12_15.xlsx",
                                  sheet = "202308311619000114")
da_tuandan_3 <- readxl::read_xlsx("D:/大客户团单-截至12_15.xlsx",
                                  sheet = "202307131026000115")

# Map "YYYY-MM-DD 场次描述" → standard datetime
mark_tuandan_changci <- function(text, jumu) {
  l   <- length(text)
  ans <- rep("", l)

  session_map <- c(
    "2023-11-03 晚场" = "2023-11-03 19:30:00",
    "2023-11-17 晚场" = "2023-11-17 19:30:00",
    "2023-11-25 午场" = "2023-11-25 15:00:00",
    "2023-11-26 午场" = "2023-11-26 15:00:00",
    "2023-12-01 晚场" = "2023-12-01 19:30:00",
    "2023-12-02 午场" = "2023-12-02 14:00:00",
    "2023-12-02 晚场" = "2023-12-02 19:30:00",
    "2023-12-03 午场" = "2023-12-03 14:00:00",
    "2023-12-05 晚场" = "2023-12-05 19:30:00",
    "2023-12-06 晚场" = "2023-12-06 19:30:00",
    "2023-12-07 晚场" = "2023-12-07 19:30:00",
    "2023-12-08 晚场" = "2023-12-08 19:30:00",
    "2023-12-09 午场" = "2023-12-09 14:00:00",
    "2023-12-09 晚场" = "2023-12-09 19:30:00",
    "2023-12-10 午场" = "2023-12-10 14:00:00",
    "2023-12-10 晚场" = "2023-12-10 19:30:00",
    "2023-12-15 晚场" = "2023-12-15 19:30:00",
    "2023-12-16 午场" = "2023-12-16 14:30:00",
    "2023-12-17 晚场" = "2023-12-17 19:30:00",
    "2023-12-31 晚场" = "2023-12-31 19:30:00",
    "2024-01-02 晚场" = "2024-01-02 19:30:00",
    "2024-01-03 晚场" = "2024-01-03 19:30:00",
    "2023-01-03 晚场" = "2024-01-03 19:30:00",  # typo in source
    "2024-01-06 晚场" = "2024-01-06 19:30:00",
    "2024-01-09 晚场" = "2024-01-09 19:30:00",
    "2024-01-10 晚场" = "2024-01-10 19:30:00",
    "2024-01-11 晚场" = "2024-01-11 19:30:00",
    "2024-01-12 晚场" = "2024-01-12 19:30:00",
    "2024-01-13 午场" = "2024-01-13 14:00:00",
    "2024-01-13 晚场" = "2024-01-13 19:30:00",
    "2024-01-14 午场" = "2024-01-14 14:00:00",
    "2024-01-14 晚场" = "2024-01-14 19:30:00",
    "2023-01-14 晚场" = "2024-01-14 19:30:00",  # typo in source
    "2024-01-16 晚场" = "2024-01-16 19:30:00",
    "2024-01-17 晚场" = "2024-01-17 19:30:00",
    "2024-01-20 午场" = "2024-01-20 14:00:00",
    "2024-01-21 午场" = "2024-01-21 14:00:00",
    "2024-01-20 晚场" = "2024-01-20 19:30:00",
    "2023-01-20 晚场" = "2024-01-20 19:30:00"   # typo in source
  )
  ans <- unname(session_map[text])
  ans[is.na(ans)] <- ""

  # MA uses 14:30 matinees (vs 14:00 for MOR)
  ma_afternoon_overrides <- c(
    "2023-11-25 午场" = "2023-11-25 14:30:00",
    "2023-11-26 午场" = "2023-11-26 14:30:00",
    "2023-12-02 午场" = "2023-12-02 14:30:00",
    "2023-12-03 午场" = "2023-12-03 14:30:00",
    "2023-12-09 午场" = "2023-12-09 14:30:00",
    "2023-12-10 午场" = "2023-12-10 14:30:00"
  )
  override_mask <- jumu == "MA" & is.element(text, names(ma_afternoon_overrides))
  ans[override_mask] <- unname(ma_afternoon_overrides[text[override_mask]])

  if (any(ans == "")) {
    print(sort(unique(text[ans == ""])))
    warning("mark_tuandan_changci: unrecognised session label(s)")
  }
  ans
}

# Map venue/city label to production code
mark_jumu_tudan <- function(text) {
  l   <- length(text)
  ans <- rep("", l)
  ans[is.element(text, c("MOR上海大剧院", "MOR北京二七", "MOR北京天桥"))] <- "MOR"
  ans[is.element(text, c("MA北京", "MA上海"))]                            <- "MA"
  if (any(ans == "")) {
    print(sort(unique(text[ans == ""])))
    warning("mark_jumu_tudan: unrecognised production label(s)")
  }
  ans
}

# Process each approval sheet and combine
process_tuandan <- function(sheet_df) {
  sheet_df |>
    filter(审批状态 != "已撤销", is.na(特殊处理)) |>
    mutate(
      剧目     = mark_jumu_tudan(剧目及城市),
      场次     = mark_tuandan_changci(paste(场次日期, 开演时间), 剧目),
      渠道类型 = "大客户",
      渠道明细 = paste0("大客户团单-", 客户名称),
      下单时间 = as.POSIXct(发起时间, tz = "UTC"),
      票价     = as.numeric(`价位(张)`),
      张数     = as.numeric(`张数(张)`)
    ) |>
    mutate(项目 = mark_project(场次, 剧目)[, 1]) |>
    dplyr::select(场次, 下单时间, 渠道类型, 渠道明细, 票价, 实收, 张数, 剧目, 项目)
}

da_tuandan_all_edit <- bind_rows(
  process_tuandan(da_tuandan_1),
  process_tuandan(da_tuandan_2),
  process_tuandan(da_tuandan_3)
) |>
  mutate(票价 = as.numeric(票价), 张数 = as.numeric(张数))

# =============================================================================
# 15. Reporting-week helpers
# =============================================================================

# Find the most recent Monday on or before a given date
set_last_monday <- function(dates) {
  vapply(as.POSIXct(dates), function(d) {
    while (weekdays(d) != "星期一") d <- d - 86400
    as.character(as.Date(d))
  }, character(1))
}

# Find the first Sunday on or after a given date
set_this_sunday <- function(dates) {
  vapply(as.POSIXct(dates), function(d) {
    while (weekdays(d) != "星期日") d <- d + 86400
    as.character(as.Date(d))
  }, character(1))
}
