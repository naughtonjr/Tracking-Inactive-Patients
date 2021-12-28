#### Nigeria LTFU Process Function Script
#### Functions generate New_LTFU, updates Continue_LTFU, and archives Partner submissions and NDR files
#### AUTHOR: Jeff Naughton (Jeffnaughton2@gmail.com)
#### CREATION DATE: 6/10/2021


prepareGO <- function(){

  # import files and put into list of dataframes
  filelist <- list.files(path = "./New files/", pattern = "*.csv")
  dflist <- list()
  for(x in filelist){
    if(str_detect(x,"line list")){
      dflist[[length(dflist)+1]] <- read.csv(paste0("./New files/",x), na.strings = "NULL")
      names(dflist)[length(dflist)] <- "df.full_line"
    }else if(str_detect(x,"ART in Q1")){
      dflist[[length(dflist)+1]] <- read.csv(paste0("./New files/",x), na.strings = "NULL")
      names(dflist)[length(dflist)] <- "df.artQ1_ltfuQ1"
    }else if(str_detect(x,"ART in Q2")){
      dflist[[length(dflist)+1]] <- read.csv(paste0("./New files/",x), na.strings = "NULL")
      names(dflist)[length(dflist)] <- "df.artQ2_ltfuQ2"
    }else if(str_detect(x,"ART in Q3")){
      dflist[[length(dflist)+1]] <- read.csv(paste0("./New files/",x), na.strings = "NULL")
      names(dflist)[length(dflist)] <- "df.artQ3_ltfuQ3"
    }else if(str_detect(x,"ART in Q4")){
      dflist[[length(dflist)+1]] <- read.csv(paste0("./New files/",x), na.strings = "NULL")
      names(dflist)[length(dflist)] <- "df.artQ4_ltfuQ4"
    }else if(str_detect(x,"end of Q1")){
      dflist[[length(dflist)+1]] <- read.csv(paste0("./New files/",x), na.strings = "NULL")
      names(dflist)[length(dflist)] <- "df.actQ1_ltfuQ2"
    }else if(str_detect(x,"end of Q2")){
      dflist[[length(dflist)+1]] <- read.csv(paste0("./New files/",x), na.strings = "NULL")
      names(dflist)[length(dflist)] <- "df.actQ2_ltfuQ3"
    }else if(str_detect(x,"end of Q3")){
      dflist[[length(dflist)+1]] <- read.csv(paste0("./New files/",x), na.strings = "NULL")
      names(dflist)[length(dflist)] <- "df.actQ3_ltfuQ4"
    }else if(str_detect(x,"end of Q4")){
      dflist[[length(dflist)+1]] <- read.csv(paste0("./New files/",x), na.strings = "NULL")
      names(dflist)[length(dflist)] <- "df.actQ4_ltfuQ1"
    }
  }
  dflist <<- dflist[names(dflist) != "df.full_line"]
  # set format date columns
  go2date <- function(x){
    x <- as.Date(x, format = "%Y-%m-%d")
  }
  dflist %>% 
    #map(~mutate_at(.x, vars(matches("date$|Date$|.date|date.")), go2date)) %>%
    list2env(globalenv())
}

setdatesGO <- function (datestart, dateend){
  currdate <<- as.Date(dateend)
  data_pull_date <<- as.Date(dateend)

  # LTFU analysis start date
  startdate <<- as.Date(datestart)
}

fixdatesGO <- function(df_name){
  datecols <- c("DATA_PULL" ,"ART_START","INACTIVE_DATE","INACTIVE_MONTH","LAST_DRUG_PICKUP_INACTIVE")
  df_name[,datecols] <- lapply(df_name[,datecols], function(x) strptime(as.character(x), "%m/%d/%Y"))
  return(df_name)
}

ltfuGO <- function(df, startdate, currdate){
  # Create new id
  df.full_line$new_id <- paste(df.full_line$datim_code, df.full_line$patient_identifier)
  # set date variables
  linelist_datecols <- c("last_drug_pickup_date", "last_drug_pickup_date_Q1", "last_drug_pickup_date_Q2", 
                          "last_drug_pickup_date_Q3", "last_drug_pickup_date_Q4")
  df.full_line[,linelist_datecols] <- lapply(df.full_line[,linelist_datecols], function(x) as.Date(x))

  # This block has been commented out because it has been replaced by a more efficient function call 
  # df.full_line$last_drug_pickup_date <- as.Date(df.full_line$last_drug_pickup_date)
  # df.full_line$last_drug_pickup_date_Q1 <- as.Date(df.full_line$last_drug_pickup_date_Q1)
  # df.full_line$last_drug_pickup_date_Q2 <- as.Date(df.full_line$last_drug_pickup_date_Q2)
  # df.full_line$last_drug_pickup_date_Q3 <- as.Date(df.full_line$last_drug_pickup_date_Q3)
  # df.full_line$last_drug_pickup_date_Q4 <- as.Date(df.full_line$last_drug_pickup_date_Q4)

  ## remove all records from df that have transferred out
  df_2 <- subset(df,  Status != "TO" & !is.na(regimen_duration))
  # create new id
  df_2$new_id <- paste(df_2$Facility.UnitUID, df_2$patient_identifier)
  # check uniqueness to see if there are duplicates
  id1 <- df_2$new_id
  length(unique(df.full_line$new_id))
  # merge with LL data 
  df_3 <-subset(df.full_line, new_id %in% id1)
  # remove deceased
  df_3$patient_has_died[is.na(df_3$patient_has_died)] <- 0
  df_3$patient_deceased_date[is.na(df_3$patient_deceased_date)] <- 0
  df_3 <-subset(df_3, patient_has_died == 0 & patient_deceased_date == 0)
  # remove transferred out
  df_3 <-subset(df_3, (patient_transferred_out == 0 | is.na(patient_transferred_out))
                      & is.na(transferred_out_date))
  # check if pickup date + regimen falls between startdate and currdate 
  df_4 <- subset(df_3,  ((last_drug_pickup_date_Q4+days_of_arv_refill_Q4+28 >= startdate)&
                         (last_drug_pickup_date_Q4+days_of_arv_refill_Q4+28 <= currdate))|
                        ((last_drug_pickup_date_Q3+days_of_arv_refill_Q3+28 >= startdate)&
                         (last_drug_pickup_date_Q3+days_of_arv_refill_Q3+28 <= currdate))|
                        ((last_drug_pickup_date_Q2+days_of_arv_refill_Q2+28 >= startdate)& 
                         (last_drug_pickup_date_Q2+days_of_arv_refill_Q2+28 <= currdate))|
                        ((last_drug_pickup_date_Q1+days_of_arv_refill_Q1+28 >= startdate)&
                         (last_drug_pickup_date_Q1+days_of_arv_refill_Q1+28 <= currdate))|
                        ((last_drug_pickup_date+days_of_arv_refill+28 >= startdate)&
                         (last_drug_pickup_date+days_of_arv_refill+28 <= currdate))
  ) 
  
}

dfcombineGO <- function(){
  colnames <- names(dflist[[1]])
  dflist <- lapply(dflist, setNames, colnames)
  df.alldata <- bind_rows(dflist)
  df.ltfu <<- ltfuGO(df.alldata, startdate, currdate)
  # combine
  df.alldata$newid <- paste(df.alldata$Facility.UnitUID, df.alldata$patient_identifier)
  id <- df.ltfu$new_id
  df.combine <- subset(df.alldata, newid %in% id)
  df.combine$art_start_date <- as.Date(df.combine$art_start_date)
  df.combine$Last.Drug.Pickup.Date <- as.Date(df.combine$Last.Drug.Pickup.Date)
  return(df.combine) 
}


#########################################
#########################################
#######       Create Final      #########
#######         LTFU DF         #########
#########################################
#########################################

df_finalGO <- function(df.combine){
  # Final dataframe column names
  col_names <- c("DATA_PULL", "NDR_PID",  "SITE_PID", "IMPLEMENTING_PARTNER", "STATE", "LGA", 
                 "FACILITY_NAME","FACILITY_UID",  "SEX",  "AGE",  "FINE_AGE", "ART_START",  "ART_TIME", "INACTIVE_DATE",
                 "INACTIVE_QTR",  "INACTIVE_MONTH", "INACTIVE_TIME",  "RETURN_VALIDATE", "LAST_DRUG_PICKUP_INACTIVE", 
                 "LAST_DRUG_MMD_INACTIVE",  "DIED_NDR", "TRANSFERRED_NDR",  "MOST_RECENT_DRUG_PICKUP", "NEW_INACTIVE", 
                 "UNRESOLVED_LiH", "UNRESOLVED_IIT")
  
  # Change current col names to final set names
  old_name <- c("Last.Drug.Pickup.Date", "regimen_duration", "art_start_date", "Implementing.Partner", "State",  
                "Facility.Name", "Facility.UnitUID", "Age", "sex")
  new_name <- c("LAST_DRUG_PICKUP_INACTIVE", "LAST_DRUG_MMD_INACTIVE", "ART_START", "IMPLEMENTING_PARTNER", "STATE",  
                "FACILITY_NAME", "FACILITY_UID", "AGE", "SEX")
  df.combine <- setnames(df.combine, old = old_name, new = new_name)
  
  ## Create Variables used in final dataframe
  df.combine <- df.combine %>%
                mutate(
                  # Data_Pull variable
                  DATA_PULL = data_pull_date,
                  # Return Validate = NO
                  RETURN_VALIDATE = "No",
                  # Died_NDR
                  DIED_NDR = NA,
                  # Transferred_NDR
                  TRANSFERRED_NDR = NA,
                  # Most recent drug pickup   
                  MOST_RECENT_DRUG_PICKUP = NA,
                  # Site PID
                  SITE_PID = df.combine$patient_identifier,
                  # Rename cols
                  NDR_PID = as.character(df.ltfu$pid[match(df.combine$newid, df.ltfu$new_id)]),
                  # Creat Fine_AGE
                  FINE_AGE = cut(AGE, 
                                  breaks = c(0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85,500), 
                                  right = FALSE, 
                                  labels = c("0-4","5-9","10-14","15-19","20-24","25-29","30-34",
                                             "35-39","40-44","45-49","50-54","55-59","60-64","65-69",
                                             "70-74","75-79","80-84","85+")),
                  # Add New_LTFU and UNRESOLVED cols
                  NEW_INACTIVE = 1,
                  UNRESOLVED_LiH = 0,
                  UNRESOLVED_IIT = 0
                  
                )
  
  
  ## Create Inactive Month, QTR, Time Variables
  # Inactive Date
  df.combine$INACTIVE_DATE <- as.Date(df.combine$LAST_DRUG_PICKUP_INACTIVE)+df.combine$LAST_DRUG_MMD_INACTIVE+28
  df.combine$INACTIVE_DATE <- as.Date(df.combine$INACTIVE_DATE, format = "%Y/%m/%d %H:%M:%S")
  # Inactive Month Generalized
  df.combine$INACTIVE_MONTH <- floor_date(df.combine$INACTIVE_DATE, unit = 'month')
  # Inactive Qtr
  df.combine$INACTIVE_QTR <- gsub(pattern = "\\.", 
                                 replacement = "Q", 
                                 x = lubridate::quarter(as.Date(df.combine$INACTIVE_DATE), with_year = TRUE, fiscal_start = 9))
  # Inactive Time variable
  INACTIVE_TIME <- (as.numeric(difftime(as.POSIXlt(df.combine$DATA_PULL),
                                       as.POSIXlt(df.combine$INACTIVE_DATE),units="weeks")))
  df.combine <- cbind(df.combine, INACTIVE_TIME)
  timebreaks <- c(0, 2, 13, 26, 100)
  timelabels <- c("< 2 Weeks", "2-4 Weeks", "1-3 Months", "4-6 Months")
  df.combine$INACTIVE_TIME <- cut(df.combine$INACTIVE_TIME, 
                                   breaks = timebreaks, 
                                   right = FALSE, 
                                   labels = timelabels)
  
  # Create ART TIME variable
  elapsed_months <- function(end_date, start_date) {
    ed <- as.POSIXlt(end_date)
    sd <- as.POSIXlt(start_date)
    12 * (ed$year - sd$year) + (ed$mon - sd$mon)
  }
  df.combine$ART_TIME <- elapsed_months(df.combine$INACTIVE_DATE, df.combine$ART_START)
  df.combine$ART_TIME <-  cut(df.combine$ART_TIME, 
                              breaks = c(0,3,7,12, 100000), 
                              right = FALSE, 
                              labels = c("< 3 months", "3-6 months", "7-11 months", "12+ months"))
  
  # Data quality check
  df.combine <- subset(df.combine, INACTIVE_DATE >= startdate)
  # df.combine$NDR_PID <- as.character(df.combine$NDR_PID)

  # reorder cols
  df.LTFU_final <<- df.combine[,c("DATA_PULL",  "NDR_PID",  "SITE_PID", "IMPLEMENTING_PARTNER", "STATE", "LGA", "FACILITY_NAME",
                                  "FACILITY_UID", "SEX",  "AGE",  "FINE_AGE", "ART_START",  "ART_TIME", "INACTIVE_DATE", "INACTIVE_QTR",
                                  "INACTIVE_MONTH", "INACTIVE_TIME",  "RETURN_VALIDATE", "LAST_DRUG_PICKUP_INACTIVE", "LAST_DRUG_MMD_INACTIVE",
                                  "DIED_NDR", "TRANSFERRED_NDR",  "MOST_RECENT_DRUG_PICKUP", "NEW_INACTIVE", "UNRESOLVED_LiH", "UNRESOLVED_IIT")]
                 
  
  df.LTFU_final$DATA_PULL <- as.Date(df.LTFU_final$DATA_PULL, format = "")
  
  # Write CSV for historical purpose
  write.csv(df.LTFU_final, paste("./Historical_LTFU/NEWLTFU_", format(currdate, '%d%b%Y'), ".csv", sep = ""),
                                   na="", row.names = FALSE)
  
}

#########################################
#########################################
#######       Create Final      #########
#######       Cont_LTFU DF      #########
#########################################
#########################################

contLTFUpathGO <- function (continue_file, newLTFU_file){
  contLTFU_path <<- paste("./Continue_LTFU/", as.character(continue_file), sep = "")
  newLTFU_path <<- paste("./Historical_LTFU/", as.character(newLTFU_file), sep= "")
}
continueltfuGO <- function(){
  df.full_line$new_id <- paste(df.full_line$datim_code, df.full_line$patient_identifier)
  df.cont_ltfu <- read_xlsx(contLTFU_path, na = "NULL")
  df.new_ltfu <- read_csv(newLTFU_path, na = "NULL")
  df.new_ltfu <- fixdatesGO(df.new_ltfu)
  df.cont_ltfu_2 <- rbind(df.cont_ltfu, df.new_ltfu)
  df.cont_ltfu_2$new_id <- paste(df.cont_ltfu_2$FACILITY_UID, df.cont_ltfu_2$SITE_PID)
  id_cont <- df.cont_ltfu_2$new_id
  id_full <- df.full_line$new_id
  
  df.check <- left_join(df.cont_ltfu_2, df.full_line, by = "new_id")
  df.return <- subset(df.check, as.Date(last_drug_pickup_date) > as.Date(LAST_DRUG_PICKUP_INACTIVE))
  id_return <- df.return$new_id
  
  df.death <-subset(df.check, patient_has_died == 1 & !is.na(patient_deceased_date)) 
  id_dead <- df.death$new_id
  
  df.transfer <-subset(df.check, patient_transferred_out == 1 & !is.na(transferred_out_date)) 
  id_transfer <- df.transfer$new_id
  
  df.newcont_ltfu <- subset(df.cont_ltfu_2, new_id %nin% id_return & new_id %nin% id_dead & new_id %nin% id_transfer)
  df.newcont_ltfu$DATA_PULL <- data_pull_date 
  
  # df.newcont_ltfu$NEW_LTFU_IIT <- 0
  df.newcont_ltfu$NEW_INACTIVE <- 0
  df.newcont_ltfu$UNRESOLVED_IIT <- 0
  df.newcont_ltfu$UNRESOLVED_LiH <- 0
  
  # Check UNRESOLVED 
  source("LTFU ArchiveContinue_JRN.R")
  source("LTFU FileRead_JRN.R")
  df_partner <<- import_partnersubmissions_old()
  
  df2.partner <- df_partner
  df2.partner$new_id <- paste(df2.partner$FACILITY_UID, df2.partner$SITE_PID)
  unresolv_df <- subset(df2.partner, !is.na(LOST_IN_TX) |
                          !is.na(DEAD) | 
                          !is.na(TRANSFERRED_OUT_NOREC) | 
                          !is.na(REACHED_REFUSE_RETURN) | 
                          !is.na(REACHED_RETURN))
  id_unresolv_lih <- unresolv_df$new_id[!is.na(unresolv_df$LOST_IN_TX)]
  id_unresolv_iit <- unresolv_df$new_id[is.na(unresolv_df$LOST_IN_TX)]
  
  df.newcont_ltfu$UNRESOLVED_LiH <- ifelse(df.newcont_ltfu$new_id %in% id_unresolv_lih, 1, 0)
  df.newcont_ltfu$UNRESOLVED_IIT <- ifelse(df.newcont_ltfu$new_id %in% id_unresolv_iit, 1, 0)
  
  elapsed_months <- function(end_date, start_date) {
    ed <- as.POSIXlt(end_date)
    sd <- as.POSIXlt(start_date)
    12 * (ed$year - sd$year) + (ed$mon - sd$mon)
  }
  
  # Categorize Time on ART with new levels
  df.newcont_ltfu$ART_TIME <- elapsed_months(df.newcont_ltfu$INACTIVE_DATE, df.newcont_ltfu$ART_START)
  df.newcont_ltfu$ART_TIME <-  cut(df.newcont_ltfu$ART_TIME, 
                                   breaks = c(0,3,7,12, 100000), 
                                   right = FALSE, 
                                   labels = c("< 3 months", "3-6 months", "7-11 months", "12+ months"))
  # Recalculate INACTIVE_TIME
  df.newcont_ltfu$INACTIVE_TIME <- (as.numeric(difftime(as.POSIXlt(df.newcont_ltfu$DATA_PULL),
                                                        as.POSIXlt(df.newcont_ltfu$INACTIVE_DATE), units="weeks")))
  df.newcont_ltfu$INACTIVE_TIME <- cut(df.newcont_ltfu$INACTIVE_TIME, 
                                       breaks = c(0, 2, 4, 13, 26, 1000), 
                                       right = FALSE, 
                                       labels = c("< 2 Weeks", "2-4 Weeks", "1-3 Months", "4-6 Months", "> 6 months"))
  
  
  df.continueLTFU_final <<- df.newcont_ltfu[,c("DATA_PULL", "NDR_PID",  "SITE_PID", "IMPLEMENTING_PARTNER", "STATE", "LGA", 
                                              "FACILITY_NAME","FACILITY_UID", "SEX",  "AGE",  "FINE_AGE", "ART_START",  "ART_TIME", "INACTIVE_DATE",
                                              "INACTIVE_QTR", "INACTIVE_MONTH", "INACTIVE_TIME",  "RETURN_VALIDATE", "LAST_DRUG_PICKUP_INACTIVE", 
                                              "LAST_DRUG_MMD_INACTIVE", "DIED_NDR", "TRANSFERRED_NDR",  "MOST_RECENT_DRUG_PICKUP", "NEW_INACTIVE", 
                                              "UNRESOLVED_LiH", "UNRESOLVED_IIT")]
  # Write CSV for historical purpose
  write.csv(df.continueLTFU_final, paste("./ContinueLTFU_",format(currdate, '%d%b%Y'),".csv", sep = ""),
              na="", row.names = FALSE)
}


LTFUcleanGO <- function(df_name){
  chng2charcols <- c("DATA_PULL" ,"ART_START", "ART_TIME", "INACTIVE_DATE","INACTIVE_MONTH","LAST_DRUG_PICKUP_INACTIVE")
  df_name[,chng2charcols] <- lapply(df_name[,chng2charcols], function(x) as.character(x))
  all <- df_name
  all$ART_START[which(is.na(all$ART_START))] <- ""
  all$INACTIVE_DATE[which(is.na(all$INACTIVE_DATE))] <- ""
  all$LAST_DRUG_PICKUP_INACTIVE[which(is.na(all$LAST_DRUG_PICKUP_INACTIVE))] <- ""


  # ==== IP ==== #
  all$IMPLEMENTING_PARTNER <- revalue(all$IMPLEMENTING_PARTNER, c("0" = "Unknown",
                                                                  "1" = "Unknown",
                                                                  "NA" = "Unknown"))
  all$IMPLEMENTING_PARTNER[which(is.na(all$IMPLEMENTING_PARTNER))] <- "Unknown"


  # ==== STATE ==== #
  all$STATE <- revalue(all$STATE, c("0" = "Unknown",
                                    "1" = "Unknown",
                                    "NA" = "Unknown"))
  all$STATE[which(is.na(all$STATE))] <- "Unknown"


  # ==== LGA ==== #
  all$LGA <- revalue(all$LGA, c("0" = "Unknown",
                                "1" = "Unknown",
                                "NA" = "Unknown"))
  all$LGA[which(is.na(all$LGA))] <- "Unknown"


  # ==== LGA ==== #
  all$FACILITY_NAME <- revalue(all$FACILITY_NAME, c("NA" = "Unknown"))
  all$FACILITY_NAME[which(is.na(all$FACILITY_NAME))] <- "Unknown"


  # ==== Clean Sex ==== #
  #all$SEX <- revalue(all$SEX, c("M" = "Male", 
  #                              "F" = "Female"))
  all$SEX[which(is.na(all$SEX))] <- "Unknown"

  # ==== Clean fine age ==== #
  all$FINE_AGE <- revalue(all$FINE_AGE, c("43834" = "1-4",
                                          "44200" = "1-4", 
                                          "1-4" = "0-4",
                                          "0 - 4" = "0-4",
                                          "43960" = "5-9",
                                          "44325" = "5-9", 
                                          "44118" = "10-14", 
                                          "44483" = "10-14",
                                          "15 - 19" = "15-19",
                                          "20 - 24" = "20-24",
                                          "25 - 29" = "25-29",
                                          "30 - 34" = "30-34",
                                          "35 - 39" = "35-39",
                                          "40 - 44" = "40-44",
                                          "45 - 49" = "45-49",
                                          "50 - 54" = "50-54", 
                                          "55 - 59" = "55-59",
                                          "60 - 64" = "60-64",
                                          "65 - 69" = "65-69",
                                          "70 - 74" = "70-74",
                                          "75 - 79" = "75-79"))
  all$FINE_AGE[which(is.na(all$FINE_AGE))] <- "Unknown"

  # ==== Clean Inactive Time ==== #
  all$ART_TIME <- revalue(all$ART_TIME, c("2 - 4 weeks" = "2-4 weeks",
                                          "2-4 Weeks" = "2-4 weeks",
                                          "1 - 3 months" = "1-3 Months",
                                          "2 - 4 months" = "2-4 Months",
                                          "4 - 6 months" = "4-6 Months",
                                          #                                        "6" = "> 6 Months",
                                          "0" = "Unknown",
                                          "1" = "Unknown"))
  all$ART_TIME[which(is.na(all$ART_TIME))] <- "Unknown"


  # ==== Clean Inactive Time ==== #
  all$ART_START <- revalue(all$ART_START, c("NA" = "",
                                            "1 - 3 months" = "1-3 Months",
                                            "2 - 4 months" = "2-4 Months",
                                            "4 - 6 months" = "4-6 Months",
                                            #                                        "6" = "> 6 Months",
                                            "0" = "Unknown",
                                            "1" = "Unknown"))
  all$ART_TIME[which(is.na(all$ART_TIME))] <- "Unknown"



  # ==== Clean Inactive Time ==== #
  all$INACTIVE_TIME <- revalue(all$INACTIVE_TIME, c("2 - 4 weeks" = "2-4 weeks",
                                                    "2-4 Weeks" = "2-4 weeks",
                                                    "1 - 3 months" = "1-3 Months",
                                                    "2 - 4 months" = "2-4 Months",
                                                    "4 - 6 months" = "4-6 Months",
                                                    #"6" = "> 6 Months",
                                                    "0" = "Unknown",
                                                    "1" = "Unknown"))
  all$INACTIVE_TIME[which(is.na(all$INACTIVE_TIME))] <- "Unknown"

  return(all)
}

###############################################
# =======    Clean Partner Reports   ======== #
###############################################

LTFUreportcleanGO <- function(df_name){

  all <- df_name

  # ==== Clean Lost in HMIS ==== #
  all$LOST_IN_TX <- revalue(all$LOST_IN_TX, 
                            c("other" = "Other",
                              "inactive due to duplicate patient records on NDR" = "Inactive due to duplicate patient records on NDR",
                              "inactive due to unsuccessful upload on NDR" = "Inactive due to unsuccessful upload on NDR",
                              "0" = "Unknown"))
  all$LOST_IN_TX[which(is.na(all$LOST_IN_TX))] <- ""


  # ==== Clean Track Attempted ==== #
  all$TRACK_ATTEMPTED <- revalue(all$TRACK_ATTEMPTED, c("Yes-tracking" = "Yes-tracking attempted",
                                                        "yes-tracking attempted" = "Yes-tracking attempted",
                                                        "No-tracking not attempted" = "No-tracking not attempted",
                                                        "1" = "Unknown",
                                                        "Inactive due to unsuccessful upload on NDR" = "Unknown"))
  all$TRACK_ATTEMPTED[which(is.na(all$TRACK_ATTEMPTED))] <- ""


  # ==== Clean Reached ==== #
  all$REACHED <- revalue(all$REACHED, c("yes-Reached" = "Yes-Reached",
                                        "Yes-reached" = "Yes-Reached",
                                        "Yes-tracking attempted" = "Yes-Reached",
                                        "no-attempted, but did not reach" = "No-attempted, but did not reach",
                                        "1" = "No-attempted, but did not reach"))
  all$REACHED[which(is.na(all$REACHED))] <- ""


  # ==== NOT_REACHED_REASON ==== #
  all$NOT_REACHED_REASON <- revalue(all$NOT_REACHED_REASON, c("Number Not reachable" = "Inaccurate phone number/address",
                                                              "Inaccurate Phone number/address" = "Inaccurate phone number/address",
                                                              "TRACKING ONGOING" = "Tracking Ongoing",
                                                              "TRacking ongoing" = "Tracking Ongoing",
                                                              "tracking ongoing" = "Tracking Ongoing",
                                                              "tracking on going" = "Tracking Ongoing",
                                                              "Tracking ongoing" = "Tracking Ongoing",
                                                              "Tracking ongoing" = "Tracking Ongoing",
                                                              "No unique ID (SITE_PID)",
                                                              "others" = "Other",
                                                              "other" = "Other",
                                                              "other" = "Other",
                                                              "Others" = "Other",
                                                              "TERMINATE CARE" = "",
                                                              "TO" = "",
                                                              "43566" = ""))
  all$NOT_REACHED_REASON[which(is.na(all$NOT_REACHED_REASON))] <- ""

  return(all)
}

###############################################
# =======  Recoded & Derived Columns ======== #
###############################################


LTFUrecodeGO <- function(df_name){

  all <- df_name 

  # ===  Recode LOST_IN_TX CATEGORIES === #

  #I changed the name from LITx to LOST_HMIS
  all <- all %>%
    mutate(LOST_HMIS = case_when(
      LOST_IN_TX == "Inactive due to duplicate patient records on NDR" ~ 1,
      LOST_IN_TX == "Inactive due to incomplete data entry to EMR" ~ 1,
      LOST_IN_TX == "Inactive due to unsuccessful upload on NDR" ~ 1,
      LOST_IN_TX == "Other" ~ 1)) %>%
    replace_na(list(LOST_HMIS = 0))


  all <- all %>%
    mutate(IIT = if_else(
      LOST_HMIS == 0, 1, 0)) #%>%
  #  replace_na(list(LOST_IN_TX = 1))


  # ==== LITx_incomplete_emr ==== #
  #Name changed to HMIS_INCOMPLETE_EMR
  all <- all %>%
    mutate(HMIS_INCOMPLETE_EMR = if_else(
      LOST_IN_TX == "Inactive due to incomplete data entry to EMR", 1, 0)) %>%
    replace_na(list(HMIS_INCOMPLETE_EMR = 0))

  # ==== LITx_ndr_unsuccessful_upload ==== #
  #Name changed
  all <- all %>%
    mutate(HMIS_NDR_UNSUCCSSFUL_UPLOAD = if_else(
      LOST_IN_TX == "Inactive due to unsuccessful upload on NDR", 1, 0)) %>%
    replace_na(list(HMIS_NDR_UNSUCCSSFUL_UPLOAD = 0))

  # ==== LITx_ndr_duplicates ==== #
  #Name changed
  all <- all %>%
    mutate(HMIS_NDR_DUPLICATES = if_else(
      LOST_IN_TX == "Inactive due to duplicate patient records on NDR", 1, 0)) %>%
    replace_na(list(HMIS_NDR_DUPLICATES = 0))

  # ==== LITx_Other ==== #
  #Name changed
  all <- all %>%
    mutate(HMIS_OTHER = if_else(
      LOST_IN_TX == "Other", 1, 0)) %>%
    replace_na(list(HMIS_OTHER = 0))

  # ==== LITx_Blank ==== #
  #Name changed; Can we use this to define IIT? See metric below (I moved it from above)
  all <- all %>%
    mutate(HMIS_BLANK = if_else(
      LOST_IN_TX == "NA", 1, 0)) %>%
    replace_na(list(HMIS_BLANK = 1))


  # ==== Interuption In Treatment ==== #
  #updated name from name change above
  #Check "!"- I wasn't sure what the "!" was for...
  #all <- all %>%
  #  mutate(IIT = if_else(
  #    HMIS_BLANK != 1, 1, 0)) %>%
  #  replace_na(list(IIT = 1))


  #updated names from name change above
  all <- all %>%
    mutate(INACTIVE_SUBSET = case_when(
      HMIS_INCOMPLETE_EMR == 1 ~ "LOST_HMIS",
      HMIS_NDR_UNSUCCSSFUL_UPLOAD == 1 ~ "LOST_HMIS",
      HMIS_NDR_DUPLICATES == 1 ~ "LOST_HMIS",
      HMIS_OTHER == 1 ~ "LOST_HMIS",
      IIT == 1 ~ "IIT"))

  # === ART TIME CATEGORIES === #
  ####################################

  all <- all %>%
    mutate(`ART_TIME: < 3 months` = if_else(
      ART_TIME == "< 3 months", 1, 0)) %>%
    replace_na(list(`ART_TIME: < 3 months` = 0))

  all <- all %>%
    mutate(`ART_TIME: 3-6 months` = if_else(
      ART_TIME == "3-6 months", 1, 0)) %>%
    replace_na(list(`ART_TIME: 3-6 months` = 0))

  all <- all %>%
    mutate(`ART_TIME: 7-11 months` = if_else(
      ART_TIME == "7-11 months", 1, 0)) %>%
    replace_na(list(`ART_TIME: 7-11 months` = 0))

  all <- all %>%
    mutate(`ART_TIME: 12+ months` = if_else(
      ART_TIME == "12+ months", 1, 0)) %>%
    replace_na(list(`ART_TIME: 12+ months` = 0))


  # === REACHED CATEGORIES === #
  ##############################

  ##################################################################################
  # ==== REACHED_Y_RETURN ==== #
  all <- all %>%
    mutate(REACHED_Y_RETURN = if_else(
      !is.na(REACHED_RETURN), 1, 0)) %>%
    replace_na(list(REACHED_Y_RETURN = 0))

  # ==== REACHED_Y_REFUSE ==== #
  all <- all %>%
    mutate(REACHED_Y_REFUSE = if_else(
      !is.na(REACHED_REFUSE_RETURN), 1, 0)) %>%
    replace_na(list(REACHED_Y_REFUSE = 0))

  # ==== REACHED_Y_DIED ==== #
  all <- all %>%
    mutate(REACHED_Y_DIED = if_else(
      !is.na(DEAD), 1, 0)) %>%
    replace_na(list(REACHED_Y_DIED = 0))

  # ==== REACHED_Y_TRANSFER ==== #
  all <- all %>%
    mutate(REACHED_Y_TRANSFER = if_else(
      !is.na(TRANSFERRED_OUT_NOREC), 1, 0)) %>%
    replace_na(list(REACHED_Y_TRANSFER = 0))

  # ==== ADDED - REACHED_Y_NOENTRY ==== #
  #all <- all %>%
  #  mutate(REACHED_Y_NOENTRY = if_else(
  #    is.na(NOT_REACHED_REASON), 1, 0))#%>%
  #  replace_na(list(REACHED_Y_NOENTRY = 0))

  # ==== ADDED - REACHED_Y_NOENTRY ==== #
  all <- all %>%
    mutate(
      REACHED_Y_NOENTRY = if_else(
        REACHED == "" &
          REACHED_Y_RETURN == 0 &
          REACHED_Y_REFUSE == 0 &
          REACHED_Y_DIED == 0 &
          REACHED_Y_TRANSFER == 0, 1, 0)) %>%
    replace_na(list(REACHED_Y_NOENTRY = 0)) 

  # ==== REACHED_Y ==== #
  #Edited to capture both yes reach response and outcomes in case no reach yes response.
  all <- all %>%
    mutate(
      REACHED_Y = if_else(
        REACHED == "Yes-Reached"|
          REACHED_Y_RETURN == 1|
          REACHED_Y_REFUSE == 1|
          REACHED_Y_DIED == 1|
          REACHED_Y_TRANSFER == 1, 1, 0)) %>%
    replace_na(list(REACHED_Y = 0)) 

  # ==== REACHED_Y_UNKNOWN

  #######################
  # ==== REACHED_N ==== #
  all <- all %>%
    mutate(
      REACHED_N = if_else(
        REACHED_Y == 0, 1, 0))# %>%

  #replace_na(list(REACHED_N = 1))


  # Reached_N Reasons

  # ==== NOT_REACHED_tracking_ongoing ==== #
  all <- all %>%
    mutate(NOT_REACHED_tracking_ongoing = if_else(
      NOT_REACHED_REASON == "Tracking ongoing", 1, 0)) %>%
    replace_na(list(NOT_REACHED_tracking_ongoing = 0))

  # ==== NOT_REACHED_no_phone_address ==== #
  all <- all %>%
    mutate(NOT_REACHED_no_phone_address = if_else(
      NOT_REACHED_REASON == "No phone number/address", 1, 0)) %>%
    replace_na(list(NOT_REACHED_no_phone_address = 0))

  # ==== NOT_REACHED_inaccurate_phone_address ==== #
  all <- all %>%
    mutate(NOT_REACHED_inaccurate_phone_address = if_else(
      NOT_REACHED_REASON == "Inaccurate phone number/address", 1, 0)) %>%
    replace_na(list(NOT_REACHED_inaccurate_phone_address = 0))

  # ==== NOT_REACHED_no_uid ==== #
  all <- all %>%
    mutate(NOT_REACHED_no_uid = if_else(
      NOT_REACHED_REASON == "No unique ID (SITE_PID)", 1, 0)) %>%
    replace_na(list(NOT_REACHED_no_uid = 0))

  # ==== NOT_REACHED_other ==== #
  all <- all %>%
    mutate(NOT_REACHED_other = if_else(
      NOT_REACHED_REASON == "Other", 1, 0)) %>%
    replace_na(list(NOT_REACHED_other = 0))

  # ==== Do we need this??? NOT_REACHED_no_entry ==== #
  all <- all %>%
    mutate(NOT_REACHED_NoEntry = if_else(
      is.na(NOT_REACHED_REASON), 1, 0))#%>%
  #  replace_na(list(NOT_REACHED_no_entry = 0))

  # === TRACK ATTEMPTED CATEGORIES === #
  # ==== IIT_TRACKED / REACHED_YES ==== #
  all <- all %>%
    mutate(IIT_TRACKED_Y = if_else(
      TRACK_ATTEMPTED == "Yes-tracking attempted" |
        REACHED == "Yes-Reached" |
        REACHED_Y_RETURN == 1 |
        REACHED_Y_REFUSE == 1 |
        REACHED_Y_DIED == 1 |
        REACHED_Y_TRANSFER == 1 |
        NOT_REACHED_tracking_ongoing == 1 |
        NOT_REACHED_no_phone_address == 1 |
        NOT_REACHED_inaccurate_phone_address == 1 |
        NOT_REACHED_no_uid == 1 |
        NOT_REACHED_other == 1,1,0)) %>%
    replace_na(list(IIT_TRACKED_Y = 0))

  # ==== EDITED IIT_TRACKED_N ==== #
  all <- all %>%
    mutate(IIT_TRACKED_N = if_else(
      IIT_TRACKED_Y != 1,1,0))

  # ==== IIT SUBSET ==== #
  all <- all %>%
    mutate(IIT_SUBSET = case_when(
      IIT_TRACKED_N == 1 ~ "IIT_TRACKED_N",
      IIT_TRACKED_Y == 1 ~ "IIT_TRACKED_Y"))


  #====Validation metric====
  all <- all %>%
    mutate(REACHED_Y_N = case_when(
      REACHED_Y == 1 ~ "REACHED_Y",
      REACHED_N == 1 ~ "REACHED_N"))

  return(all)

}

#======= Collapse into Facility-Level dataset=======#

LTFUcollapseGO <- function(df_name){

  df <- df_name %>% 
    group_by(FACILITY_UID, SEX, FINE_AGE, DATA_PULL, IMPLEMENTING_PARTNER, 
             STATE, LGA, FACILITY_NAME, ART_TIME, INACTIVE_TIME) %>% 
    dplyr::summarise(INACTIVE_PID_N = n(), 
                     LOST_HMIS=sum(LOST_HMIS),
                     IIT=sum(IIT), 
                     HMIS_INCOMPLETE_EMR=sum(HMIS_INCOMPLETE_EMR), 
                     HMIS_NDR_UNSUCCSSFUL_UPLOAD=sum(HMIS_NDR_UNSUCCSSFUL_UPLOAD), 
                     HMIS_NDR_DUPLICATES=sum(HMIS_NDR_DUPLICATES), 
                     HMIS_OTHER=sum(HMIS_OTHER), 
                     HMIS_BLANK=sum(HMIS_BLANK), 
                     `ART_TIME: < 3 months`=sum(`ART_TIME: < 3 months`), 
                     `ART_TIME: 3-6 months`=sum(`ART_TIME: 3-6 months`), 
                     `ART_TIME: 7-11 months`=sum(`ART_TIME: 7-11 months`), 
                     `ART_TIME: 12+ months`=sum(`ART_TIME: 12+ months`),
                     REACHED_Y_RETURN=sum(REACHED_Y_RETURN), 
                     REACHED_Y_REFUSE=sum(REACHED_Y_REFUSE), 
                     REACHED_Y_DIED=sum(REACHED_Y_DIED),
                     REACHED_Y_TRANSFER=sum(REACHED_Y_TRANSFER), 
                     REACHED_Y_NOENTRY=sum(REACHED_Y_NOENTRY), 
                     REACHED_Y=sum(REACHED_Y), 
                     REACHED_N=sum(REACHED_N), 
                     NOT_REACHED_tracking_ongoing=sum(NOT_REACHED_tracking_ongoing), 
                     NOT_REACHED_no_phone_address=sum(NOT_REACHED_no_phone_address), 
                     NOT_REACHED_inaccurate_phone_address=sum(NOT_REACHED_inaccurate_phone_address), 
                     NOT_REACHED_no_uid=sum(NOT_REACHED_no_uid), 
                     NOT_REACHED_other=sum(NOT_REACHED_other),
                     NOT_REACHED_NoEntry=sum(NOT_REACHED_NoEntry), 
                     IIT_TRACKED_Y=sum(IIT_TRACKED_Y), 
                     IIT_TRACKED_N=sum(IIT_TRACKED_N), 
                     LiH_New=sum(LiH_New, na.rm = TRUE), 
                     IIT_New=sum(IIT_New,  na.rm = TRUE), 
                     UNRESOLVED_LiH=sum(UNRESOLVED_LiH,  na.rm = TRUE), 
                     UNRESOLVED_IIT=sum(UNRESOLVED_IIT,  na.rm = TRUE))
  return(df)
}

LTFUcollapseGO.2 <- function(df_name){
  
  df <- df_name %>% 
    group_by(FACILITY_UID, SEX, FINE_AGE, DATA_PULL, IMPLEMENTING_PARTNER, 
             STATE, LGA, FACILITY_NAME, ART_TIME, INACTIVE_TIME) %>% 
    dplyr::summarise(INACTIVE_PID_N = n(), 
                     LOST_HMIS=sum(LOST_HMIS),
                     IIT=sum(IIT), 
                     HMIS_INCOMPLETE_EMR=sum(HMIS_INCOMPLETE_EMR), 
                     HMIS_NDR_UNSUCCSSFUL_UPLOAD=sum(HMIS_NDR_UNSUCCSSFUL_UPLOAD), 
                     HMIS_NDR_DUPLICATES=sum(HMIS_NDR_DUPLICATES), 
                     HMIS_OTHER=sum(HMIS_OTHER), 
                     HMIS_BLANK=sum(HMIS_BLANK), 
                     `ART_TIME: < 3 months`=sum(`ART_TIME: < 3 months`), 
                     `ART_TIME: 3-6 months`=sum(`ART_TIME: 3-6 months`), 
                     `ART_TIME: 7-11 months`=sum(`ART_TIME: 7-11 months`), 
                     `ART_TIME: 12+ months`=sum(`ART_TIME: 12+ months`),
                     REACHED_Y_RETURN=sum(REACHED_Y_RETURN), 
                     REACHED_Y_REFUSE=sum(REACHED_Y_REFUSE), 
                     REACHED_Y_DIED=sum(REACHED_Y_DIED),
                     REACHED_Y_TRANSFER=sum(REACHED_Y_TRANSFER), 
                     REACHED_Y_NOENTRY=sum(REACHED_Y_NOENTRY), 
                     REACHED_Y=sum(REACHED_Y), 
                     REACHED_N=sum(REACHED_N), 
                     NOT_REACHED_tracking_ongoing=sum(NOT_REACHED_tracking_ongoing), 
                     NOT_REACHED_no_phone_address=sum(NOT_REACHED_no_phone_address), 
                     NOT_REACHED_inaccurate_phone_address=sum(NOT_REACHED_inaccurate_phone_address), 
                     NOT_REACHED_no_uid=sum(NOT_REACHED_no_uid), 
                     NOT_REACHED_other=sum(NOT_REACHED_other),
                     NOT_REACHED_NoEntry=sum(NOT_REACHED_NoEntry), 
                     IIT_TRACKED_Y=sum(IIT_TRACKED_Y), 
                     IIT_TRACKED_N=sum(IIT_TRACKED_N), 
                     LiH_New=sum(LiH_New, na.rm = TRUE), 
                     IIT_New=sum(IIT_New,  na.rm = TRUE), 
                     UNRESOLVED_LiH=sum(UNRESOLVED_LiH,  na.rm = TRUE), 
                     UNRESOLVED_IIT=sum(UNRESOLVED_IIT,  na.rm = TRUE),
                     TX_CURR=sum(TX_CURR/INACTIVE_PID_N,  na.rm = TRUE),
                     TX_NEW=sum(TX_NEW/INACTIVE_PID_N,  na.rm = TRUE))
  return(df)
}



#######################################################
######## Move LTFU data and Partner submissions #######
########          to archive folders            #######
#######################################################

    

moveLTFUdataGO <- function(){
  NDRfiles <- list.files(path = "./New files/", pattern = "*.csv")
    for(filename in NDRfiles){
      if(str_detect(filename,"line list")){
        file.rename(
          from = file.path("./New files", filename),
          to = file.path("../Historical Linelists", filename))
      }else{
        file.rename(
          from = file.path("./New files", filename),
          to = file.path("./Historical NDR Files", filename))
      }
    }
}

    
    
moveSubdataGO <- function(){
  prtnrsubfiles <- list.files(path = "./Partner Submissions/", pattern = "*.xlsx")
  file.rename(
    from = file.path("./Partner Submissions/", prtnrsubfiles[grep("APIN", prtnrsubfiles)]),
    to = file.path("./Historical Tools/APIN/", prtnrsubfiles[grep("APIN", prtnrsubfiles)]))
  file.rename(
    from = file.path("./Partner Submissions/", prtnrsubfiles[grep("IHVN", prtnrsubfiles)]),
    to = file.path("./Historical Tools/IHVN/", prtnrsubfiles[grep("IHVN", prtnrsubfiles)]))
  file.rename(
    from = file.path("./Partner Submissions/", prtnrsubfiles[grep("CIHP", prtnrsubfiles)]),
    to = file.path("./Historical Tools/CIHP/", prtnrsubfiles[grep("CIHP", prtnrsubfiles)]))
  file.rename(
    from = file.path("./Partner Submissions/", prtnrsubfiles[grep("CCFN", prtnrsubfiles)]),
    to = file.path("./Historical Tools/CCFN/", prtnrsubfiles[grep("CCFN", prtnrsubfiles)]))
}
      
ndr_read <- function(f_name){
  print(paste("Reading", f_name, "..."))
  df <- read_excel(f_name, 
                   col_types = c("date", #DATA_PULL
                                 "text", #NDR_PID
                                 "text", #SITE_PID
                                 "text", #IMPLEMENTING_PARTNER
                                 "text", #STATE
                                 "text", #LGA
                                 "text", #FACILITY_NAME
                                 "text", #FACILITY_UID
                                 "text", #SEX
                                 "numeric", #AGE
                                 "text", #FINE_AGE
                                 "date", #ART_START
                                 "text", #ART_TIME
                                 "date", #INACTIVE_DATE
                                 "text", #INACTIVE_QTR
                                 "date", #INACTIVE_MONTH
                                 "text", #INACTIVE_TIME
                                 "text", #RETURN_VALIDATE
                                 "date", #DATE_DRUG_PICKUP
                                 "numeric", #DRUG_MMD
                                 "date", #DIED_NDR
                                 "date", #TRANSFERRED_NDR
                                 "date", #MOST_RECENT_DRUG_PICKUP
                                 "numeric", #NEW_INACTIVE
                                 "numeric", #UNRESOLVED_IIT
                                 "numeric" #UNRESOLVED_LiH
                   ))
  print(colnames(df))
  return(df)
}


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

read_partnerALT <- function(x, sheet_name){
  print(paste("Reading", sheet_name, "..."))
  partnerfile <- readxl::read_xlsx(paste0("./Partner Submissions/",x), sheet = sheet_name,
                                   skip = 2,
                                   col_names = T,
                                   col_types = c(
                                     "text", #NDR_PID
                                     "text", #SITE_PID
                                     "text", #SEX
                                     "text", #AGE
                                     "text", #FINE_AGE
                                     "date", #DATA_PULL
                                     "text", #IMPLEMENTING_PARTNER
                                     "text", #STATE
                                     "text", #LGA
                                     "text", #FACILITY_NAME
                                     "text", #FACILITY_UID
                                     "date", #ART_START
                                     "text", #ART_TIME
                                     "date", #INACTIVE_DATE
                                     "text", #INACTIVE_QTR
                                     "date", #INACTIVE_MONTH
                                     "text", #INACTIVE_TIME
                                     "text", #RETURN_VALIDATE
                                     "date", #DATE_DRUG_PICKUP
                                     "text", #DRUG_MMD
                                     "date", #MOST_RECENT_DRUG_PICKUP
                                     "date", #DIED_NDR
                                     "date", #TRANSFERRED_NDR
                                     "numeric", #NEW_INACTIVE
                                     "numeric", #UNRESOLVED_IIT
                                     "numeric", #UNRESOLVED_LiH
                                     "text", #LOST_IN_TX
                                     "text", #TRACKING_ATTEMPTED
                                     "text", #REACHED
                                     "date", #REACHED_REFUSE_RETURN
                                     "date", #REACHED_RETURN
                                     "date", #DEAD
                                     "date", #TRANSFERRED_OUT_NOREC
                                     "text", #NOT_REACHED
                                     "text" #NARRATIVE
                                   ))
  print(colnames(partnerfile))
  return(partnerfile)
}

import_partnersubmissions_ALT <- function(){
  filelist <- list.files(path = "./Partner Submissions/", pattern = "*.xlsx")
  print(filelist)
  master <- data.frame()
  for(x in filelist){
    if(str_detect(x,"APIN")){
      print("Reading APIN...")
      master <- rbind(master,
                      read_partnerALT(x, "Plateau"),
                      read_partnerALT(x, "Osun"),
                      read_partnerALT(x, "Ondo"),
                      read_partnerALT(x, "Benue"),
                      read_partnerALT(x, "Ekiti"),
                      read_partnerALT(x, "Ogun"),
                      read_partnerALT(x, "Oyo")
      )
    }else if(str_detect(x,"CCFN")){
      print("Reading CCFN...")
      master <- rbind(master,
                      read_partnerALT(x, "Enugu"),
                      read_partnerALT(x, "Imo"),
                      read_partnerALT(x, "Ebonyi"),
                      read_partnerALT(x, "Delta")
      )
    }else if(str_detect(x,"CIHP")){
      print("Reading CIHP...")
      master <- rbind(master,
                      read_partnerALT(x, "Kaduna"),
                      read_partnerALT(x, "Gombe"),
                      read_partnerALT(x, "Lagos"),
                      read_partnerALT(x, "Kogi")
      )
    }else if(str_detect(x,"IHVN")){
      print("Reading IHVN...")
      master <- rbind(master,
                      read_partnerALT(x, "Nasarawa"),
                      read_partnerALT(x, "Katsina"),
                      read_partnerALT(x, "FCT"),
                      read_partnerALT(x, "Rivers")
      )
    }
  }
  
  master <- master %>% filter(!is.na(IMPLEMENTING_PARTNER)) %>%
    distinct()
  
  print(unique(master$IMPLEMENTING_PARTNER))
  print(unique(master$STATE))
  
  print("Final counts by implementing partner and state:")
  print(master %>% group_by(IMPLEMENTING_PARTNER,STATE) %>% summarise(n = n())
  )
  print("Final distinct counts by implementing partner and state:")
  print(master %>% distinct(SITE_PID,FACILITY_UID,IMPLEMENTING_PARTNER,STATE) %>% group_by(IMPLEMENTING_PARTNER,STATE) %>% summarise(n = n())
  )
  
  return(master)
}
