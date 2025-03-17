-- Tao ds dai ly thang:

rollback;
commit;
select *  from ttkd_bsc.dm_daily_khdn 
--    update ttkd_bsc.dm_daily_khdn  set MANV_QLDAILY = 'CTV087171'
where thang=202502 and ma_daily in ('GTGT00189')--,'GTGT00050','GTGT00116','GTGT00136','GTGT00144')
;
		
		----insert daily moi tu excel
		insert into ttkd_bsc.dm_daily_khdn (THANG, MA_DAILY, TEN_DAILY, MANV_QLDAILY, THANG_KYHD, LOAI_HOPDONG)		
					select 202406 thang, ma_nv  MA_DAILY, ten_nv TEN_DAILY, 'CTV077959' MANV_QLDAILY, 202406 THANG_KYHD, 'DAI LY MOI' LOAI_HOPDONG
					from admin_hcm.nhanvien where ma_nv = 'GTGT00178'
		;
		---update maTo, PB, VTCV
		update ttkd_bsc.dm_daily_khdn a
			set (ma_to, ma_pb, ma_vtcv, ten_vtcv)= (select ma_to, ma_pb, ma_vtcv, ten_vtcv from ttkd_bsc.nhanvien where ma_nv = a.MANV_QLDAILY and thang = a.thang)
			where a.thang=202502;
	commit;
	;
-- Dai ly moi:
		insert into ttkd_bsc.dm_daily_khdn (ma_daily,ten_daily,ma_pb, ma_to, manv_qldaily, thang, thang_kyhd, loai_hopdong )
		   
		   select distinct ma_daily, (select upper(ten_nv) from admin_hcm.nhanvien_onebss where ma_nv=a.ma_daily) ten_daily,
					 ma_pb, ma_to, ma_nv, 202502, 202502, 'DAI LY MOI'	---thang n
		    from 
					   (select distinct
									 (case when ma_tiepthi like 'GT%' or ma_tiepthi like 'DL%' then ma_tiepthi
											  when ma_nguoigt like 'GT%' or ma_nguoigt like 'DL%' then ma_nguoigt  
											  when nguoi_gt like 'GT%' or nguoi_gt like 'DL%' then nguoi_gt end) ma_daily
									 ,(case when dichvuvt_id!=2 then (select ma_dv from admin_hcm.donvi where donvi_id=a.pbh_nhan_id) 
										else ma_pb end) ma_pb, ma_to
									 ,(case when substr(ma_tiepthi,1,3) in ('VNP','CTV') then ma_tiepthi
											  when substr(ma_nguoigt,1,3) in ('VNP','CTV') then ma_nguoigt end) ma_nv                            
						  from ttkd_bsc.ct_bsc_ptm a
						  where thang_ptm =202502		--thang n
										 and (((ma_tiepthi like 'GT%' or ma_tiepthi like 'DL%') and ma_tiepthi not in (select ma_daily from ttkd_bsc.dm_daily_khdn))
												 or ((ma_nguoigt like 'GT%' or ma_nguoigt like 'DL%') and ma_nguoigt not in (select ma_daily from ttkd_bsc.dm_daily_khdn))
												 or ((nguoi_gt like 'GT%' or nguoi_gt like 'DL%') and nguoi_gt not in (select ma_daily from ttkd_bsc.dm_daily_khdn)))
						union all
								select nguoi_gt, ma_pb, '','' 
								from ttkd_bsc.dt_ptm_vnp_202502 		---thang n, file anh Tuyen tao
								where nguoi_gt like 'GT%' or nguoi_gt like 'DL%' 
					   )a 
					where ma_daily not in (select ma_daily from ttkd_bsc.dm_daily_khdn)
			   
			   ;