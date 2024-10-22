update ttkd_bsc.ct_bsc_ptm a
		    set 
					thang_tlkpi = thang_tldg_dt
					,thang_tlkpi_to = thang_tldg_dt
					,thang_tlkpi_phong = thang_tldg_dt
--		       select thang_luong, thang_ptm, ma_tb, nguon, thang_tlkpi, thang_tlkpi_to, thang_tlkpi_phong, thang_tldg_dt, doanhthu_kpi_nvptm, doanhthu_kpi_to, doanhthu_kpi_phong, lydo_khongtinh_dongia from ttkd_bsc.ct_bsc_ptm  a
		    where (loaitb_id not in (21) or ma_kh='GTGT rieng') 
						and nvl(thang_tlkpi, 0) <> 999999		---cac th ngoai le khong tinh KPI, chi tinh DONGIA
						and nvl(thang_tlkpi, 999999) >= 202409
						and thang_tldg_dt = 202409
--						and thang_tlkpi is null
--						and thang_luong = 86
						
		 --   and ma_tb = 'hcm_ca_00067906'
		 
		    ;
		    commit;


		update ttkd_bsc.ct_bsc_ptm a
		    set thang_tlkpi_hotro = thang_tldg_dt_nvhotro
		    
		--     select thang_luong, thang_ptm, ma_tb, nguon, thang_tlkpi, thang_tlkpi_to, thang_tlkpi_phong, thang_tldg_dt, thang_tldg_dt_nvhotro, thang_tlkpi_hotro, lydo_khongtinh_dongia, nop_du, mien_hsgoc, manv_ptm, manv_hotro from ttkd_bsc.ct_bsc_ptm  a
		    where (loaitb_id not in (21) or ma_kh='GTGT rieng') 
						and nvl(thang_tlkpi_hotro, 0) <> 999999
						and nvl(thang_tlkpi_hotro, 999999) >= 202409
						and thang_tldg_dt_nvhotro = 202409
		--				and thang_luong = 86
		
		;
	
    commit;


update ttkd_bsc.ct_bsc_ptm a
   set 
		thang_tlkpi_dnhm = thang_tldg_dnhm
		 , thang_tlkpi_dnhm_to=thang_tldg_dnhm
		 , thang_tlkpi_dnhm_phong=thang_tldg_dnhm
--     select thang_luong, thang_ptm,ma_tb, nguon, thang_tldg_dnhm, thang_tlkpi_dnhm, thang_tlkpi_dnhm_to, thang_tlkpi_dnhm_phong, doanhthu_kpi_dnhm, lydo_khongtinh_dongia, nop_du, mien_hsgoc, manv_ptm, manv_hotro from ttkd_bsc.ct_bsc_ptm  a   
		   where (loaitb_id not in (21) or ma_kh='GTGT rieng') 
						and nvl(thang_tlkpi_dnhm, 0) <> 999999
						and nvl(thang_tlkpi_dnhm, 999999) >= 202409
						and thang_tldg_dnhm = 202409 
						and nvl(thang_tlkpi_dnhm_phong, 999999) >= 202409
						
--						and thang_luong = 86
		;
   rollback;
   commit;
-- Ky n 
-- to: 
-- DL_CNT  - DT cua AM QLDL (DN1) ko ghi nhan cho Truong line ngoai tru Dai ly thu tuc CNT
				-- DT cua AM QLDL (DN2, 3) ghi nhan cho Truong line vi khong co To QLDL
	
-- to - dnhm DL_CNT
--update ttkd_bsc.ct_bsc_ptm set thang_tlkpi_dnhm_to='' where thang_ptm=202404
--;
			 	--Chi 1 thang 7, 8, 9 theo eo VANBAN_ID
				update ttkd_bsc.ct_bsc_ptm a  
								set thang_tlkpi_dnhm = 0		---khong tinh BSC cho NVPTM
--										, ghi_chu = ghi_chu || '; ' || 'khong '
--						select * from ttkd_bsc.ct_bsc_ptm a
						where thang_luong = 71
						
				;
			update ttkd_bsc.ct_bsc_ptm a 
			   set thang_tlkpi_dnhm = a.thang_ptm, thang_tlkpi_dnhm_phong = a.thang_ptm
						, thang_tlkpi_dnhm_to = case when MA_NGUOIGT  in (select MA_DAILY 
																						from ttkd_bsc.dm_daily_khdn 
																						where ma_pb = 'VNP0702300' and ma_daily not in ('dl_cnt', 'DL_CNT','GTGT00001') and thang = a.thang_ptm)
																					then 0
																			else a.thang_ptm end
--		     select doanhthu_kpi_dnhm, doanhthu_kpi_dnhm_phong, ma_tb, manv_ptm, ten_pb, dich_vu, heso_daily, thang_tlkpi_dnhm, thang_tlkpi_dnhm_to, thang_tldg_dnhm, tien_dnhm, thang_ptm, trangthai_tt_id, lydo_khongtinh_dongia from ttkd_bsc.ct_bsc_ptm a
		    where thang_ptm = 202409
							and nvl(thang_tlkpi_dnhm, 999999) >= a.thang_ptm		 ---thang n
							and (loaitb_id not in (21,210, 131, 222) or ma_kh='GTGT rieng')   
							and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%' )
										)
						  and chuquan_id in (145, 266, 264)
						  and (tien_dnhm > 0 or tien_sodep > 0) 
						  and ((loaitb_id not in (20,149, 21)  and trangthai_tt_id=1 ) or loaitb_id in (20,149))
						  and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
						and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
						
				  ;

		----tinh cac th khong quet dk ghi nhan hso goc
		---khong tinh dai ly ca nhan dongia (aNguyen dang tinh), nguoc lai tinh KPI cho dai ly ca nhan
		update ttkd_bsc.ct_bsc_ptm a 
		   set thang_tlkpi = a.thang_ptm, thang_tlkpi_to = a.thang_ptm, thang_tlkpi_phong = a.thang_ptm
					, thang_tlkpi_hotro =  a.thang_ptm
--		     select thang_tlkpi_hotro, thang_ptm, doanhthu_kpi_nvptm, doanhthu_kpi_to, doanhthu_kpi_phong, ma_tb, ten_pb, ma_nguoigt, loai_ld , ma_to, thang_tldg_dt, thang_tlkpi, thang_tlkpi_to, thang_tlkpi_phong, lydo_khongtinh_luong, lydo_khongtinh_dongia from ttkd_bsc.ct_bsc_ptm a
			where a.thang_ptm = 202409
							and (loaitb_id not in (210, 21,131) or ma_kh='GTGT rieng') and chuquan_id in (145, 266, 264)
				  and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id) 
				 and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%' )
							)
				  and nvl(thang_tlkpi, 999999)  >= a.thang_ptm 
				  and dthu_goi >0
				  and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
--				  and heso_daily is not null and heso_daily >0

;				  
commit;		
				   
				  
 
 rollback;
 commit;
-- Ky (n-1)  
		update ttkd_bsc.ct_bsc_ptm a 
			   set thang_tlkpi_dnhm = 202409, thang_tlkpi_dnhm_phong = 202409
						, thang_tlkpi_dnhm_to = case when MA_NGUOIGT  in (select MA_DAILY 
																						from ttkd_bsc.dm_daily_khdn 
																						where ma_pb = 'VNP0702300' and ma_daily not in ('dl_cnt', 'DL_CNT','GTGT00001') and thang = a.thang_ptm)
																					then 0
																			else 202409 end
--		     select doanhthu_kpi_dnhm, doanhthu_kpi_dnhm_phong, ma_tb, manv_ptm, ten_pb, dich_vu, heso_daily, thang_tlkpi_dnhm, thang_tlkpi_dnhm_to, thang_tldg_dnhm, tien_dnhm, thang_ptm, trangthai_tt_id, lydo_khongtinh_dongia from ttkd_bsc.ct_bsc_ptm a
		where thang_ptm = 202408 ---thang n-1
						and nvl(thang_tlkpi_dnhm, 999999) >= 202409		 ---thang n
						and (loaitb_id not in (21,210, 222) or ma_kh='GTGT rieng')   
						 and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%')
								)
						 and chuquan_id in (145,266,264)     
						 and (tien_dnhm>0 or tien_sodep>0)
						 and ((loaitb_id not in (20,149, 21)  and trangthai_tt_id=1 ) or loaitb_id in (20,149))
						 and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
						and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
						and nocuoc_n1 is null
--						and ma_tb = 'fvn_lamb808'
		;
		
		----tinh cac th khong quet dk ghi nhan hso goc
		---khong tinh dai ly ca nhan dongia (aNguyen dang tinh), nguoc lai tinh KPI cho dai ly ca nhan
		update ttkd_bsc.ct_bsc_ptm a 
		   set thang_tlkpi = 202409, thang_tlkpi_to = 202409, thang_tlkpi_phong = 202409
				, thang_tlkpi_hotro = 202409
--		     select thang_ptm, doanhthu_kpi_nvptm, doanhthu_kpi_to, doanhthu_kpi_phong, ma_tb, ten_pb, ma_nguoigt, loai_ld , ma_to, heso_daily, thang_tldg_dt, thang_tlkpi, thang_tlkpi_to, thang_tlkpi_phong, lydo_khongtinh_luong, lydo_khongtinh_dongia from ttkd_bsc.ct_bsc_ptm a
			where thang_ptm = 202408 ---thang n-1
						and nvl(thang_tlkpi, 999999) >= 202409
						and chuquan_id in (145,266, 264) 
						and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
						and (loaitb_id<>21 or ma_kh='GTGT rieng')
						and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%')
							)
						and ( trangthaitb_id_n1=1 or (loaitb_id=20 and trangthaitb_id_n1=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146))) or loaitb_id in (89,90,146) )
						and dthu_goi >0
						and (nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)>0 
									or (nvl(dthu_ps_n1, 0) =0 and loaitb_id in (55,80,116,117,140,132,122,288,181,290,292,175,302) 
													and thang_bddc > a.thang_ptm		---thang n
										)
							) 
						and nocuoc_n1 is null 
						and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149)) ---VNPts khong xet trang thai TT
--						and ma_tb = 'fvn_lamb808'

			;
-- Ky (n-2)
			update ttkd_bsc.ct_bsc_ptm a 
			   set thang_tlkpi_dnhm = 202409, thang_tlkpi_dnhm_phong = 202409
						, thang_tlkpi_dnhm_to = case when MA_NGUOIGT  in (select MA_DAILY 
																						from ttkd_bsc.dm_daily_khdn 
																						where ma_pb = 'VNP0702300' and ma_daily not in ('dl_cnt', 'DL_CNT','GTGT00001') and thang = a.thang_ptm)
																					then 0
																			else 202409 end
--		     select doanhthu_kpi_dnhm, doanhthu_kpi_dnhm_phong, ma_tb, manv_ptm, ten_pb, dich_vu, heso_daily, thang_tlkpi_dnhm, thang_tlkpi_dnhm_to, thang_tldg_dnhm, tien_dnhm, thang_ptm, trangthai_tt_id, lydo_khongtinh_dongia from ttkd_bsc.ct_bsc_ptm a
			where thang_ptm = 202407 	---thang n-2
						and nvl(thang_tlkpi_dnhm, 999999) >= 202409
						and (loaitb_id not in (21,210, 222) or ma_kh='GTGT rieng') and chuquan_id in (145,266,264)
						 and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%' )
								)
					  and (tien_dnhm>0 or tien_sodep>0)
					  and ((loaitb_id not in (20,149)  and trangthai_tt_id=1 ) or loaitb_id in (20,149))					  
					  and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
					  and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
					  and nocuoc_n2 is null 
		;
			----tinh cac th khong quet dk ghi nhan hso goc
		---khong tinh dai ly ca nhan dongia (aNguyen dang tinh), nguoc lai tinh KPI cho dai ly ca nhan
		update ttkd_bsc.ct_bsc_ptm a 
		   set thang_tlkpi = 202409, thang_tlkpi_to = 202409, thang_tlkpi_phong = 202409
				, thang_tlkpi_hotro = 202409
--		     select thang_ptm, doanhthu_kpi_nvptm, doanhthu_kpi_to, doanhthu_kpi_phong, ma_tb, ten_pb, ma_nguoigt, loai_ld , ma_to, heso_daily, thang_tldg_dt, thang_tlkpi, thang_tlkpi_to, thang_tlkpi_phong, lydo_khongtinh_luong, lydo_khongtinh_dongia from ttkd_bsc.ct_bsc_ptm a
			where thang_ptm = 202407 	---thang n-2
							and nvl(thang_tlkpi, 999999) >= 202409  ---thang n
							and (loaitb_id<>21 or ma_kh='GTGT rieng') and chuquan_id in  (145,266, 264)
							and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
							and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%')
									)
							  and ( trangthaitb_id_n2=1 or (loaitb_id=20 and trangthaitb_id_n2=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146))) or loaitb_id in (89,90,146) )
							  and dthu_goi >0
							  and (nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)+nvl(dthu_ps_n2,0)>0 
											or (nvl(dthu_ps_n2, 0) =0 and loaitb_id in (55,80,116,117,140,132,122,288,181,290,292,175,302) 
															 and thang_bddc >= a.thang_ptm        	
													)
									) 
							and nocuoc_n2 is null  
							  and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
							  
			;
commit;
rollback;
-- Ky n-3
			update ttkd_bsc.ct_bsc_ptm a 
			   set thang_tlkpi_dnhm = 202409, thang_tlkpi_dnhm_phong = 202409
						, thang_tlkpi_dnhm_to = case when MA_NGUOIGT  in (select MA_DAILY 
																						from ttkd_bsc.dm_daily_khdn 
																						where ma_pb = 'VNP0702300' and ma_daily not in ('dl_cnt', 'DL_CNT','GTGT00001') and thang = a.thang_ptm)
																					then 0
																			else 202409 end
--		     select doanhthu_kpi_dnhm, doanhthu_kpi_dnhm_phong, ma_tb, manv_ptm, ten_pb, dich_vu, heso_daily, thang_tlkpi_dnhm, thang_tlkpi_dnhm_to, thang_tldg_dnhm, tien_dnhm, thang_ptm, trangthai_tt_id, lydo_khongtinh_dongia from ttkd_bsc.ct_bsc_ptm a
			where thang_ptm = 202406 		---thang n-3
							and nvl(thang_tlkpi_dnhm, 999999) >= 202409
							and (loaitb_id not in (21,210, 222) or ma_kh='GTGT rieng')  and chuquan_id in (145,266,264) 
							and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%' )
								)
							 and (tien_dnhm>0 or tien_sodep>0)
							 and ((loaitb_id not in (20,149)  and trangthai_tt_id=1 ) or loaitb_id in (20,149))
							 and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
							 and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
							 and nocuoc_n3 is null 
			;
				----tinh cac th khong quet dk ghi nhan hso goc
		---khong tinh dai ly ca nhan dongia (aNguyen dang tinh), nguoc lai tinh KPI cho dai ly ca nhan
		update ttkd_bsc.ct_bsc_ptm a 
		   set thang_tlkpi = 202409, thang_tlkpi_to = 202409, thang_tlkpi_phong = 202409
				, thang_tlkpi_hotro = 202409
--		     select id, thang_ptm, doanhthu_kpi_nvptm, doanhthu_kpi_to, doanhthu_kpi_phong, ma_tb, ten_pb, ma_nguoigt, loai_ld , ma_to, heso_daily, thang_tldg_dt, thang_tlkpi, thang_tlkpi_to, thang_tlkpi_phong, lydo_khongtinh_luong, lydo_khongtinh_dongia from ttkd_bsc.ct_bsc_ptm a
			where thang_ptm = 202406		---thang n-3
								and nvl(thang_tlkpi, 999999) >= 202409
								and (loaitb_id<>21 or ma_kh='GTGT rieng') and chuquan_id in  (145,266, 264)
								and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%')
									) 
								and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
								and (trangthaitb_id_n3=1 or (loaitb_id=20 and trangthaitb_id_n3=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146)))or loaitb_id in (89,90,146) )												  
								and dthu_goi >0
								and (nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)+nvl(dthu_ps_n2,0)+nvl(dthu_ps_n3,0)>0 
											or (nvl(dthu_ps_n2, 0) =0 and loaitb_id in (55,80,116,117,140,132,122,288,181,290,292,175,302) 
															and thang_bddc > a.thang_ptm		--thang n
												)
									) 
								and nocuoc_n3 is null					
								and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
				;
commit;
		
-- Kiem tra
select * from ttkd_bsc.ct_bsc_ptm 
    where thang_ptm>=202402 and (loaitb_id<>21 or ma_kh='GTGT rieng')
        and thang_tlkpi_to is null and thang_tlkpi=202407;


select * from ttkd_bsc.ct_bsc_ptm 
    where thang_ptm>=202402 
        and (loaitb_id<>21 or ma_kh='GTGT rieng') and ma_to is not null 
        and (lydo_khongtinh_dongia is null
                    or (lydo_khongtinh_dongia not like '%Phat trien qua Dai ly%'
                            and lydo_khongtinh_dongia not like '%ptm kenh CTVXHH%'))
        and thang_tlkpi_to=202407 and thang_tldg_dt is null
        and (loai_ld not like '_LCN' or loai_ld is null)  ;

    
-- tinh kpi, ko tinh don gia
select thang_ptm, ten_pb, ten_to,dich_vu, ma_tb, trangthaitb_id_n3, nocuoc_n3, ngay_luuhs_ttkd,nop_du, sothang_dc
        ,loai_ld, thang_tldg_dt , lydo_khongtinh_dongia, thang_tlkpi, thang_tlkpi_to, thang_tlkpi_phong
    from ttkd_bsc.ct_bsc_ptm 
    where thang_ptm>=202402
        and (loaitb_id<>21 or ma_kh='GTGT rieng') and lydo_khongtinh_dongia not like '%don gia%'
        and manv_ptm is not null 
        and ((thang_tldg_dt is null and thang_tlkpi=202407)
                or (thang_tldg_dt =202407 and thang_tlkpi is null)) ;   

             
select * from ttkd_bsc.ct_bsc_ptm
where chuquan_id in (145,266)
    and ( thang_tldg_dt=202407 or thang_tlkpi=202407 or thang_tlkpi_to=202407 or thang_tlkpi_phong=202407)
    and loaitb_id not in (20,21,149) and loaihd_id=1
    and (trangthai_tt_id is null or trangthai_tt_id=0);


select * from ttkd_bsc.ct_bsc_ptm_pgp
where (thang_tlkpi_hotro=202407 and thang_tldg_dt_nvhotro is null)
        or (thang_tldg_dt_nvhotro=202407 and thang_tlkpi_hotro is null );


select * from ttkd_bsc.ct_bsc_ptm_pgp
where lydo_khongtinh_dongia like '%Dai ly%' 
        and lydo_khongtinh_dongia not like '%No cuoc%'
        and lydo_khongtinh_dongia not like '%Trang thai%'
        and lydo_khongtinh_dongia not like '%doanh thu%'
        and manv_hotro is not null and thang_tldg_dt_nvhotro is null ;


select * from ttkd_bsc.ct_bsc_ptm a
    where chuquan_id in (145,266)
        and ( thang_tldg_dt=202407 or thang_tlkpi=202407 or thang_tlkpi_to=202407 or thang_tlkpi_phong=202407 or thang_tldg_dnhm=202407 or thang_tldg_dt_nvhotro = 202407)
        and loaitb_id not in (20,21,149) and loaihd_id=1
        and (trangthai_tt_id is null or trangthai_tt_id=0);
	   
select * from ttkd_bsc.ct_bsc_ptm a
    where chuquan_id in (145,266)
        and  ((thang_tldg_dt is null and thang_tldg_dt_nvhotro = 202407)
					--or (thang_tldg_dt = 202407 and thang_tldg_dt_nvhotro is null)
					or (thang_tldg_dt is null and THANG_TLKPI = 202407)
				)
	   ;


  

    
    
