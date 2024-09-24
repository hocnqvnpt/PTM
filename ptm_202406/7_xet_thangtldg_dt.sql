			----BSUNG ma nhan vien tiep thi
			UPDATE ttkd_bsc.ct_bsc_ptm a
										set manv_ptm = (select y.ma_nv 
																	from css_hcm.hd_khachhang x
																						join admin_hcm.nhanvien_onebss y on x.ctv_id = y.nhanvien_id
																		where x.hdkh_id = a.hdkh_id)
												--, thang_luong = 101
--					select thang_luong from ttkd_bsc.ct_bsc_ptm a
								where manv_ptm is null
											and exists (select 1 from css_hcm.hd_khachhang where ctv_id is not null and hdkh_id = a.hdkh_id
																		and exists (select 1 from ttkd_bsc.nhanvien where ctv_id = nhanvien_id))
											and thang_ptm >= 202403
											and thang_tldg_dt is null			--- chua tinh luong
							;
							
			-----CODE UPDATE NV MOI
				update ttkd_bsc.ct_bsc_ptm a 
							    set (tennv_ptm, ma_to, ten_to, ma_pb, ten_pb, ma_vtcv, loai_ld, nhom_tiepthi)
									= (select TEN_NV, MA_TO, TEN_TO, MA_PB, TEN_PB, MA_VTCV, LOAI_LD, NHOMLD_ID
											from 
													(select ma_nv, b.ten_nv, b.ma_to, b.ten_to, b.ma_pb, b.ten_pb, b.ma_vtcv, b.loai_ld, b.nhomld_id
																, row_number() over (partition by ma_nv order by thang desc) rnk
													  from ttkd_bsc.nhanvien b--, ttkd_bsc.dm_phongban pb 
													  where b.thang >= 202403 --thang n-3
																	
													  ) where rnk = 1 and ma_nv = a.manv_ptm
											) 
--			select * from ttkd_bsc.ct_bsc_ptm a
							    where  thang_ptm >= 202403 and tennv_ptm is null and ma_vtcv is null and nvl(thang_tldg_dt, 999999) >= 202406 and nvl(loaitb_id, 0) <> 21
											and exists (select 1	from ttkd_bsc.nhanvien b
																		where b.thang >= 202403 --thang n-3
																							and b.ma_nv = a.manv_ptm)
						;
				----END
							
							
				update ttkd_bsc.ct_bsc_ptm a 
							    set (tennv_ptm, ma_to, ten_to, ma_pb, ten_pb, ma_vtcv, loai_ld, nhom_tiepthi)
									= (select b.ten_nv, b.ma_to, b.ten_to, b.ma_pb, b.ten_pb, b.ma_vtcv, b.loai_ld, b.nhomld_id
										  from ttkd_bsc.nhanvien b--, ttkd_bsc.dm_phongban pb 
										  where b.thang = to_number(to_char(trunc(sysdate, 'month') - 1, 'yyyymm')) --thang n
														and b.ma_nv = a.manv_ptm
										  ) 
--								, thang_luong = 101
--			select * from ttkd_bsc.ct_bsc_ptm a
							    where  a.manv_ptm is not null and ma_pb is null
												and exists (select 1 from css_hcm.hd_khachhang where ctv_id is not null and hdkh_id = a.hdkh_id
																		and exists (select 1 from ttkd_bsc.nhanvien where ctv_id = nhanvien_id))
												and thang_ptm >= 202403 
												and thang_tldg_dt is null			--- chua tinh luong
			;
			----END BSUNG ma nhan vien tiep thi
commit;
rollback;
-- ID447: cap nhat ktoan kh xac nhan thu tien
	update ttkd_bsc.ct_bsc_ptm a 
		   set (thang_luong, xacnhan_khkt, thang_xacnhan_khkt, thang_tldg_dt, thang_tlkpi, thang_tlkpi_to, thang_tlkpi_phong, thang_tldg_dt_nvhotro, thang_tlkpi_hotro) =       
				 (select 7, tienthu_khkt, to_char(ngaycapnhat_khkt,'yyyymm') , 202406, 202406, 202406, 202406, 
					    case when manv_hotro is not null then 202406 end ,
					    case when manv_hotro is not null then 202406 end
					from ttkdhcm_ktnv.amas_duan_ngoai_doanhthu where id=a.id_447)
--	     select * from ttkd_bsc.ct_bsc_ptm a
	    where  thang_ptm >= 202403 and thang_xacnhan_khkt is null ---thang n-3
					--and nvl(thang_tldg_dt, 202405) = 202405
			and exists(select 1 from ttkdhcm_ktnv.amas_duan_ngoai_doanhthu where ngaythanhtoan_khkt is not null and id=a.id_447)
			;
   ---1 dot 1, 2
   
 
-- Ptm qua Digishop:
		update ttkd_bsc.ct_bsc_ptm a 
		    set thang_tldg_dt=999999, thang_tldg_dnhm=999999
--	  select thang_tldg_dt, thang_tldg_dnhm, heso_hotro_nvptm, heso_hotro_nvhotro from ttkd_bsc.ct_bsc_ptm a 
		    where thang_ptm = 202406 and ungdung_id=17 and (nhom_tiepthi<>2 or nhom_tiepthi is null)
		    ;
        
       ---2  dot 1, 2
-- ******************Ky n ***************** 
			---chay khi co yeu cau
--			update ttkd_bsc.ct_bsc_ptm set thang_tldg_dt=null
--			    where thang_ptm=202404 and (loaitb_id<>21 or ma_kh='GTGT rieng')
			    ;
---3		luot (neu khong co yeu cau)
    -- Dot 1:    dot 2 khong chay
		update ttkd_bsc.ct_bsc_ptm a 
			set thang_tldg_dt = 202406, lydo_khongtinh_dongia=null
--		     select ma_tb, doituong_id,  lydo_khongtinh_luong, nocuoc_ptm, trangthaitb_id, dichvuvt_id, loaitb_id, thoihan_id, mien_hsgoc, nop_du, lydo_khongtinh_dongia, thang_tldg_dt from ttkd_bsc.ct_bsc_ptm a  
		    where thang_ptm = 202406 
							and (loaitb_id not in (20, 21,131, 358) or ma_kh='GTGT rieng')  and (chuquan_id is null or chuquan_id <> 264)
						  and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)						  
						  and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%' and heso_daily=0.05 and MA_NGUOIGT is not null)
							)
							and thang_tldg_dt is null 
						    and (dthu_ps > 0 
									or (nvl(dthu_ps, 0) =0 and loaitb_id in (55,80,116,117,140,132,122,288,181,290,292,175,302) 
													and dthu_goi >0 and thang_bddc > 202406)			--thang n
							) 
						and nocuoc_ptm is null 		
						  and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
						  and  (trangthaitb_id=1 or (loaitb_id=20 and trangthaitb_id=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146))) or loaitb_id in (89,90,146) )            
						  and (nop_du=1 or mien_hsgoc is not null) 
						  and (loai_ld not like '_LCN' or loai_ld is null)
						  and manv_ptm is not null
						  and not exists (select 1 from ttkd_bct.ptm_codinh_202406
												where ma_pb='VNP0703000' and nguoi_cn_goc in ('myvnpt','shop.vnpt.vn','dhtt.mytv', 'freedoo') and thuebao_id=a.thuebao_id )
						 
				;
---4  dot 2 khong chay

create index ttkd_bsc.idx_thangptm on ttkd_bsc.ct_bsc_ptm (thang_ptm);

    -- Dot 2:    
		update ttkd_bsc.ct_bsc_ptm a 
		    set thang_tldg_dt = 202406
--		    select thang_luong, thang_ptm, ma_gd, thuebao_id, ma_tb, manv_ptm, loai_ld, ma_pb, lydo_khongtinh_luong, lydo_khongtinh_dongia, luong_dongia_nvptm, dthu_goi, dthu_ps, nocuoc_ptm, ten_pb, trangthaitb_id, loaitb_id, thoihan_id, ngay_luuhs_ttkd, ngay_luuhs_ttvt, nop_du, mien_hsgoc, thang_tldg_dt from ttkd_bsc.ct_bsc_ptm a  
				 where thang_ptm = 202406 
--							thang_luong = 87
							and (loaitb_id not in (210, 21,131) or ma_kh='GTGT rieng') and (chuquan_id is null or chuquan_id<>264) 
				  and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id) 
				 -- and lydo_khongtinh_luong is null 
				 and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%' and heso_daily=0.05 and MA_NGUOIGT is not null)
							)
				  and nvl(thang_tldg_dt, 999999)  >= 202406 
--				  and thang_tldg_dt is null
--				  and a.ma_tb in ('hcm_econtract_00000999')
				  and (dthu_ps > 0 
									or (nvl(dthu_ps, 0) =0 and loaitb_id in (55,80,116,117,140,132,122,288,181,290,292,175,302) 
													and dthu_goi >0 and thang_bddc > a.thang_ptm
										)
							) 
				and nocuoc_ptm is null 
				  and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
				  and  (trangthaitb_id=1 or (loaitb_id=20 and trangthaitb_id=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146))) or loaitb_id in (89,90,146) )            
				  and (nop_du=1 or mien_hsgoc is not null) 
				  and (loai_ld not like '_LCN' or loai_ld is null)	---khong tinh dai ly ca nhan dongia (aNguyen dang tinh), nguoc lai tinh KPI
--				  and manv_ptm is not null
				 
				  ;
 ---5 
 
    -- ACT
--		update ttkd_bsc.ct_bsc_ptm a set thang_tldg_dt=null
--		    where chuquan_id=264 and lydo_khongtinh_luong is null and thang_tldg_dt=202404
--		    ;
         --6

		update ttkd_bsc.ct_bsc_ptm a 
			   set thang_tldg_dt = 202406, thang_tlkpi=202406, thang_tlkpi_to=202406, thang_tlkpi_phong=202406
				    ,thang_tldg_dnhm=case when tien_dnhm>0 and ngay_tt is not null then 202406 end
				    ,thang_tlkpi_dnhm=case when tien_dnhm>0 and ngay_tt is not null then 202406 end
--		     select  thang_ptm, ma_gd, thuebao_id, ma_tb, manv_ptm, loai_ld, ma_pb, lydo_khongtinh_luong, lydo_khongtinh_dongia, luong_dongia_nvptm
--					, dthu_goi, dthu_ps, nocuoc_ptm, ten_pb, trangthaitb_id, loaitb_id, thoihan_id, ngay_luuhs_ttkd, ngay_luuhs_ttvt, nop_du, mien_hsgoc
--					, thang_tldg_dt, thang_tlkpi, thang_tlkpi_to, thang_tlkpi_phong, thang_tldg_dnhm, thang_tlkpi_dnhm    from ttkd_bsc.ct_bsc_ptm a
		    where thang_ptm>=202403 and chuquan_id=264 --and manv_ptm is not null
			   and lydo_khongtinh_luong is null and thang_xacnhan_khkt is not null 
			   and nvl(thang_tldg_dt, 999999)  >= 202406
			   
			   ; 
		---7

-- VNPTS cua BHOL tiep thi:
--		update ttkd_bsc.ct_bsc_ptm set thang_tldg_dt_dai=null
--		    where thang_ptm=202404 and loaitb_id<>21 and manv_tt_dai is not null and thang_tldg_dt_dai is not null
--		    ;
		--8
		update ttkd_bsc.ct_bsc_ptm a 
			set thang_tldg_dt_dai = a.thang_ptm
--		     select thang_tldg_dt_dai, thang_tldg_dt, lydo_khongtinh_luong, heso_daily  from ttkd_bsc.ct_bsc_ptm a
		    where thang_ptm = 202406 and loaitb_id<>21
				  and nvl(thang_tldg_dt_dai, 999999)  >= 202406 
				  and dthu_ps is not null and nocuoc_ptm is null                       
				  and (trangthaitb_id=1 or (loaitb_id=20 and trangthaitb_id=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146))) or loaitb_id in (89,90,146) )
				  and (nop_du=1 or mien_hsgoc is not null)      
				  and not exists (select 1 from ttkd_bsc.nhanvien where thang = a.thang_ptm and loai_ld like '_LCN' and ma_nv = a.manv_tt_dai) 
						and ( (thang_tldg_dt is null and lydo_khongtinh_luong like '%Phat trien qua Dai ly%')			---PGP dc tinh khi ban qua dai ly
										or thang_tldg_dt is not null
									)
					
							 
				  
				  ;
		----9

-- PGP
--		update ttkd_bsc.ct_bsc_ptm set thang_tldg_dt_nvhotro=null
--		    where thang_ptm=202404 and (loaitb_id not in (21,131) or ma_kh='GTGT rieng')
--					  and manv_hotro is not null and thang_tldg_dt_nvhotro is not null
--					  ;
		--10
    commit;
    
		update ttkd_bsc.ct_bsc_ptm a 
			   set thang_tldg_dt_nvhotro = a.thang_ptm
--		    select  thang_luong, ma_tb, manv_ptm, manv_hotro, thang_tldg_dt, thang_tldg_dt_nvhotro, luong_dongia_nvptm, lydo_khongtinh_luong, lydo_khongtinh_dongia, nguon, ma_gd_gt, tyle_hotro, nop_du, mien_hsgoc, heso_daily  from ttkd_bsc.ct_bsc_ptm a
		    where thang_ptm = 202406 --and ma_tb = 'hcm_econtract_00000999'
								and (loaitb_id not in (210, 21,131) or ma_kh='GTGT rieng') and (chuquan_id is null or chuquan_id<>264) 
							  and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id) 
							and ( (thang_tldg_dt is null and lydo_khongtinh_luong like '%Phat trien qua Dai ly%')			---PGP dc tinh khi ban qua dai ly
										or thang_tldg_dt is not null
									)
							  and nvl(thang_tldg_dt_nvhotro, 999999)  >= 202406 

							  and (dthu_ps > 0 
												or (nvl(dthu_ps, 0) =0 and loaitb_id in (55,80,116,117,140,132,122,288,181,290,292,175,302) 
																and dthu_goi >0 and thang_bddc > a.thang_ptm
													)
										) 
							and nocuoc_ptm is null 
							  and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
							  and  (trangthaitb_id=1 or (loaitb_id=20 and trangthaitb_id=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146))) or loaitb_id in (89,90,146) )            
							  and (nop_du=1 or mien_hsgoc is not null) 
--							  and manv_hotro is not null
--							  and thang_luong = 26
						

--				  and exists(select 1 from ttkd_bsc.nhanvien where thang = a.thang_ptm and ma_pb='VNP0702600' and ma_nv = a.manv_hotro)
				  
				;
		--11

   -- dnhm:  
--			update ttkd_bsc.ct_bsc_ptm a set thang_tldg_dnhm=null
--			    where thang_ptm = 202404 and thang_tldg_dnhm = 202404
			    ;
			    --12
        
		update ttkd_bsc.ct_bsc_ptm a 
			   set thang_tldg_dnhm = a.thang_ptm
--		     select chuquan_id, ma_tb, manv_ptm, ten_pb, thang_tldg_dt, thang_tldg_dnhm, tien_dnhm, tien_tt, thang_ptm,  to_char(ngay_tt,'yyyymm'), ngay_tt, soseri, trangthai_tt_id from ttkd_bsc.ct_bsc_ptm a
		    where thang_ptm = 202406 
							and (loaitb_id not in (20,21,210, 222) or ma_kh='GTGT rieng')   
						  and thang_tldg_dnhm is null 
						  and thang_tldg_dt = a.thang_ptm
						  -- and lydo_khongtinh_luong is null 
								 and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%' and heso_daily=0.05 and MA_NGUOIGT is not null)
										)
						  and chuquan_id in (145,266,264)
						  and (tien_dnhm>0 or tien_sodep>0) and ngay_tt is not null and trangthai_tt_id=1
						  and (loai_ld not like '_LCN' or loai_ld is null)         
						  and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
				
				  ;
				  --13
                
 
 -- dnhm vnpts: 
			 update ttkd_bsc.ct_bsc_ptm a 
				   set thang_tldg_dnhm = thang_ptm
--	 		select chuquan_id, ma_tb, manv_ptm, ten_pb, thang_tldg_dt, thang_tldg_dnhm, tien_dnhm, tien_tt, thang_ptm,  to_char(ngay_tt,'yyyymm'), ngay_tt, soseri, trangthai_tt_id from ttkd_bsc.ct_bsc_ptm a

			    where thang_ptm = 202406 and loaitb_id=20
				   and thang_tldg_dt = a.thang_ptm
				   and nvl(thang_tldg_dnhm, 999999) >= 202406   
				   and lydo_khongtinh_luong is null
				   and (tien_dnhm>0 or tien_sodep>0) 
				   and (loai_ld not like '_LCN' or loai_ld is null) 
			;
               --14     
commit;
--***************** ky n-1 ******************
--			update ttkd_bsc.ct_bsc_ptm a 
--			    set thang_tldg_dt=''
--			 where thang_ptm=202403 ----thang n-1
--						and (loaitb_id<>21 or ma_kh='GTGT rieng') and thang_tldg_dt=202404		--thang n
--			 ; 
		--15

-- Dot 1: dot 2 khong chay
			update ttkd_bsc.ct_bsc_ptm a 
			    set thang_tldg_dt = 202406		--thang n
--			     select ma_gd, ma_tb, dthu_ps,dthu_ps_n1, trangthaitb_id_n1, thang_tldg_dt   from ttkd_bsc.ct_bsc_ptm a 
			    where thang_ptm = 202405		--thang n-1
						and (loaitb_id not in (21,131) or ma_kh='GTGT rieng') and chuquan_id in (145,266) 
					  and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
					  and thang_tldg_dt is null and lydo_khongtinh_luong is null
					  and ( trangthaitb_id_n1=1 or (loaitb_id=20 and trangthaitb_id_n1=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146))) or loaitb_id in (89,90,146) )         
					  and (nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)>0 
									or (nvl(dthu_ps_n1, 0) =0 and loaitb_id in (55,80,116,117,140,132,122,288,181,290,292,175,302) 
													and dthu_goi >0 and thang_bddc > 202406			---thang n
											)
							) 
					and nocuoc_n1 is null 
					  and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
					  and (nop_du=1 or mien_hsgoc is not null) 
					  and (loai_ld not like '_LCN' or loai_ld is null)
					--  and ma_tb in ('hcmtn3','phien3c64')
					  ;
			--16  dot 2 khong chay
			commit;
			
-- dnhm 
--			update ttkd_bsc.ct_bsc_ptm a set thang_tldg_dnhm=null
--			    where thang_ptm=202403 ---thang n-1
--							and thang_tldg_dnhm=202404			--thang n
--			    ;
			    --17
        
		update ttkd_bsc.ct_bsc_ptm a 
					set thang_tldg_dnhm = 202406		--thang n
--		     select chuquan_id, ma_tb, manv_ptm, ten_pb, thang_tldg_dt,  thang_tldg_dnhm, tien_dnhm, tien_tt, thang_ptm,  to_char(ngay_tt,'yyyymm'), ngay_tt, soseri, trangthai_tt_id from ttkd_bsc.ct_bsc_ptm a
		    where thang_ptm=202405 ---thang n-1
					and (loaitb_id not in (21,210, 222) or ma_kh='GTGT rieng')  and chuquan_id in (145,266,264) 
					 and thang_tldg_dnhm is null 
					 and thang_tldg_dt = 202406   --thang n          
					 -- and lydo_khongtinh_luong is null 
						 and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%' and heso_daily=0.05 and MA_NGUOIGT is not null)
								)
					 
					 and (tien_dnhm>0 or tien_sodep>0)
					 and ((loaitb_id not in (20,149)  and ngay_tt is not null and trangthai_tt_id=1 ) or loaitb_id in (20,149))
					 and (loai_ld not like '_LCN' or loai_ld is null)         
					 and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
					 
					 ;
				--18

                      
-- Dot 2:
--			update ttkd_bsc.ct_bsc_ptm a 
--			    set thang_tldg_dt='' 
--			 where thang_ptm =202403 ---thang n-1
--						and thang_tldg_dt=202404 ---thang n
--			 ;
			--19
 
			update ttkd_bsc.ct_bsc_ptm a 
			    set thang_tldg_dt = 202406 ---thang n
--			select ma_tb, dich_vu, lydo_khongtinh_luong, lydo_khongtinh_dongia, manv_ptm, loai_ld, tien_dnhm, thang_tldg_dnhm,  thang_tldg_dt from ttkd_bsc.ct_bsc_ptm a 
			 where thang_ptm = 202405 ---thang n-1
					and (chuquan_id in (145,266) or chuquan_id is null) 
--					and ma_tb = 'vannhi8375228'
--					and thang_tldg_dt is null 
					--and nvl(thang_tldg_dt, 0) <>  999999			---ca th bi khoa lai
					and nvl(thang_tldg_dt, 999999) >= 202406
				   and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
				   and (loaitb_id<>21 or ma_kh='GTGT rieng')
				  and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%' and heso_daily=0.05 and MA_NGUOIGT is not null)
							)
--							  and thuebao_id = 12251580	
				   and ( trangthaitb_id_n1=1 or (loaitb_id=20 and trangthaitb_id_n1=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146))) or loaitb_id in (89,90,146) )
				
				     and (nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)>0 
									or (nvl(dthu_ps_n1, 0) =0 and loaitb_id in (55,80,116,117,140,132,122,288,181,290,292,175,302) 
													and dthu_goi >0 and thang_bddc > a.thang_ptm		---thang n
										)
							) 
					and nocuoc_n1 is null 
				   and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149)) ---VNPts khong xet trang thai TT
				   and (nop_du=1 or mien_hsgoc is not null) 
				   and (loai_ld not like '_LCN' or loai_ld is null)
			
--					and manv_ptm is not null
--			and thang_luong = 17
				   ;
				   --20
     

			update ttkd_bsc.ct_bsc_ptm a 
				set thang_tldg_dnhm = 202406 ---thang n
--			     select lydo_khongtinh_dongia, chuquan_id, ma_tb, manv_ptm, ten_pb, thang_tldg_dt,  thang_tldg_dnhm, tien_dnhm, tien_tt, thang_ptm,  to_char(ngay_tt,'yyyymm'), ngay_tt, soseri, trangthai_tt_id from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm = 202405 ---thang n-1
						and (loaitb_id not in (21,210, 222) or ma_kh='GTGT rieng')   
						 and thang_tldg_dt = 202406 --- thang n       
						 
--						 and nvl(thang_tldg_dnhm, 0) <>  999999			---cac th bi khoa lai
						and nvl(thang_tldg_dnhm, 999999) >= 202406
						 and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%' and heso_daily=0.05 and MA_NGUOIGT is not null)
								)
						 and chuquan_id in (145,266,264)     
						 and (tien_dnhm>0 or tien_sodep>0)
						 and ((loaitb_id not in (20,149)  and ngay_tt is not null and trangthai_tt_id=1 ) or loaitb_id in (20,149))
						 and (loai_ld not like '_LCN' or loai_ld is null)         
						 and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
						 
				;
						 ---21
                
                            
-- VNPTS cua BHOL:
--				update ttkd_bsc.ct_bsc_ptm a 
--				    set thang_tldg_dt_dai=''
--				    where thang_ptm=202403  ---thang n-1
--								and thang_tldg_dt_dai=202404 ---thang n
--								and manv_tt_dai is not null and loaitb_id<>21 
--				;
				--22
      
			update ttkd_bsc.ct_bsc_ptm a 
			    set thang_tldg_dt_dai = 202406		---thang n
--			 select * from ttkd_bsc.ct_bsc_ptm a 
			   where thang_ptm = 202405 	---thang n-1
					  and thang_tldg_dt = 202406		---thang n
					  and nvl(thang_tldg_dt_dai, 999999) >= 202406		---thang n
					and manv_tt_dai is not null and loaitb_id<>21 
				   and (loaitb_id<>21 or ma_kh='GTGT rieng')
				   and lydo_khongtinh_luong is null and thang_tldg_dt_dai is null  
				   and (nop_du=1 or mien_hsgoc is not null) 
				   and not exists (select 1 from ttkd_bsc.nhanvien where thang = 202406 and loai_ld like '_LCN' and ma_nv=a.manv_tt_dai)
				   
				   ;
			--23

-- PGP
--			update ttkd_bsc.ct_bsc_ptm 
--					set thang_tldg_dt_nvhotro=null
--					    where thang_ptm=202403	--thang n-1
--								 and thang_tldg_dt_nvhotro=202404		--thang n
--								and (loaitb_id not in (21,131) or ma_kh='GTGT rieng')
--							  and manv_hotro is not null
--						;
				--24
        
			update ttkd_bsc.ct_bsc_ptm a 
			    set thang_tldg_dt_nvhotro = 202406			--thang n
--			    select ma_tb, thang_tldg_dt, thang_tldg_dt_nvhotro, heso_daily, lydo_khongtinh_dongia, luong_dongia_nvptm, luong_dongia_nvhotro, manv_hotro from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm = 202405		--thang n-1
							and (loaitb_id not in (210, 21,131) or ma_kh='GTGT rieng') and (chuquan_id is null or chuquan_id<>264)
							  and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
								
								and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%')		---PGP dc tinh khi ban qua dai ly
												)
							and ( (thang_tldg_dt is null and lydo_khongtinh_luong like '%Phat trien qua Dai ly%')			---PGP dc tinh khi ban qua dai ly
										or thang_tldg_dt = 202406				---tam thoi PTM < T5, sau do chuyen ve is not null sau >= T5
									)
							  and nvl(thang_tldg_dt_nvhotro, 999999) >= 202406
							  and ( trangthaitb_id_n1=1 or (loaitb_id=20 and trangthaitb_id_n1=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146))) or loaitb_id in (89,90,146) )
							  and nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)>0 and nocuoc_n1 is null      
							  and ((loaitb_id not in (20,149) and trangthai_tt_id=1 ) or loaitb_id in (20,149))
							  and (nop_du = 1 or mien_hsgoc is not null) 
							 -- and exists(select 1 from ttkd_bsc.nhanvien where thang = 202406 and ma_nv=a.manv_hotro)
--					and thang_luong = 26
				;
			--25
			commit;
-- ******************ky n-2 ****************
--			update ttkd_bsc.ct_bsc_ptm a 
--			    set thang_tldg_dt=''
--			 where thang_ptm=202402 ---thang n-2
--						and thang_tldg_dt=202404 ---thang n
--						and (loaitb_id<>21 or ma_kh='GTGT rieng')
			 ;
			--26
 
 -- Dot 1:  dot 2 khong chay
			 update ttkd_bsc.ct_bsc_ptm a 
			    set thang_tldg_dt = 202406		---thang n
			    -- select * from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm = 202404 		--thang n-2
						and (loaitb_id not in (21,131) or ma_kh='GTGT rieng') and chuquan_id in (145,266) 
					  and (loaitb_id<>21 or ma_kh='GTGT rieng') 
					  and lydo_khongtinh_luong is null and thang_tldg_dt is null  
					  and ( trangthaitb_id_n2=1 or (loaitb_id=20 and trangthaitb_id_n2=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146))) or loaitb_id in (89,90,146) )
					   and (nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)+nvl(dthu_ps_n2,0)>0 
									or (nvl(dthu_ps_n2, 0) =0 and loaitb_id in (55,80,116,117,140,132,122,288,181,290,292,175,302) 
													and dthu_goi >0 and thang_bddc > 202406 --thang n
										)
							) 
					and nocuoc_n2 is null  
					  
					  and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
					  and (nop_du=1 or mien_hsgoc is not null) 
					  and (loai_ld not like '_LCN' or loai_ld is null)
				;
				--27  dot 2 khong chay

--				update ttkd_bsc.ct_bsc_ptm a set thang_tldg_dnhm=null
--				    where thang_ptm=202402		--thang n-2
--								and thang_tldg_dnhm=202404		--thang n
				    ;
				--28
				
			update ttkd_bsc.ct_bsc_ptm a 
					set thang_tldg_dnhm = 202406	--thang n
			    -- select chuquan_id, ma_tb, manv_ptm, ten_pb, thang_tldg_dt,  thang_tldg_dnhm, tien_dnhm, tien_tt, thang_ptm,  to_char(ngay_tt,'yyyymm'), ngay_tt, soseri, trangthai_tt_id from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm = 202404 ---thang n-2
							and thang_tldg_dt = 202406		---thang n
						and (loaitb_id not in (21,210, 222) or ma_kh='GTGT rieng')   
						 and thang_tldg_dnhm is null              
						 -- and lydo_khongtinh_luong is null 
						 and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%' and heso_daily=0.05 and MA_NGUOIGT is not null)
								)
						 and chuquan_id in (145,266,264)     
						 and (tien_dnhm>0 or tien_sodep>0)
						 and ((loaitb_id not in (20,149)  and ngay_tt is not null and trangthai_tt_id=1 ) or loaitb_id in (20,149))
						 and (loai_ld not like '_LCN' or loai_ld is null)         
						 and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
			 ;
			 ---END dot 1
			 ---29
            commit;    
                   
 -- Dot 2:
			update ttkd_bsc.ct_bsc_ptm a 
			    set thang_tldg_dt = 202406		---thang n
--			     select thang_luong, ma_tb, ma_pb, loai_ld, thoihan_id, manv_ptm, thang_tldg_dt, lydo_khongtinh_luong, lydo_khongtinh_dongia from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm = 202404 	---thang n-2
--							and thang_tldg_dt is null 
							  and (loaitb_id<>21 or ma_kh='GTGT rieng') and (chuquan_id in  (145,266) or chuquan_id is null)
							 
						--	and nvl(thang_tldg_dt, 0) <>  999999			---cac th bi khoa lai
							and nvl(thang_tldg_dt, 999999) >= 202406
						   and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
						  and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%' and heso_daily=0.05 and MA_NGUOIGT is not null)
								)
							  and ( trangthaitb_id_n2=1 or (loaitb_id=20 and trangthaitb_id_n2=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146))) or loaitb_id in (89,90,146) )
							  and (nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)+nvl(dthu_ps_n2,0)>0 
											or (nvl(dthu_ps_n2, 0) =0 and loaitb_id in (55,80,116,117,140,132,122,288,181,290,292,175,302) 
															and dthu_goi >0 and thang_bddc > 202406        		--thang n
													)
									) 
							and nocuoc_n2 is null  
							  and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
							  and (nop_du=1 or mien_hsgoc is not null) 
							  and (loai_ld not like '_LCN' or loai_ld is null)
--					  and manv_ptm is not null
				;
				--30


--			update ttkd_bsc.ct_bsc_ptm a 
--			    set thang_tldg_dnhm=null
--			    where thang_ptm=202402 	---thang n-2
--							and thang_tldg_dnhm=202404				---thang n
			;
			--31
        
			update ttkd_bsc.ct_bsc_ptm a 
			    set thang_tldg_dnhm = 202406	---thang n
--			     select chuquan_id, ma_tb, manv_ptm, ten_pb, thang_tldg_dt,  thang_tldg_dnhm, tien_dnhm, tien_tt, thang_ptm,  to_char(ngay_tt,'yyyymm'), ngay_tt, soseri, trangthai_tt_id from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm = 202404 	---thang n-2
						and thang_tldg_dt = 202406 	---thang n
					and (loaitb_id not in (21,210, 222) or ma_kh='GTGT rieng') and chuquan_id in (145,266,264)
					  and nvl(thang_tldg_dnhm, 999999) >= 202406
				
						 and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%' and heso_daily=0.05 and MA_NGUOIGT is not null)
								)
					  and (tien_dnhm>0 or tien_sodep>0)
					  and ((loaitb_id not in (20,149)  and ngay_tt is not null and trangthai_tt_id=1 ) or loaitb_id in (20,149))
					  and (loai_ld not like '_LCN' or loai_ld is null)         
					  and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
--					  and manv_ptm is not null
			;
			--32
                  
     
-- VNPTS cua BHOL:
			update ttkd_bsc.ct_bsc_ptm a 
			    set thang_tldg_dt_dai = 202406		--thang n
--			  select * from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm = 202404		--thang n-2
					and thang_tldg_dt = 202406	--thang n
					and nvl(thang_tldg_dt_dai, 999999) >=202406 
				   and (loaitb_id<>21 or ma_kh='GTGT rieng')
				   and lydo_khongtinh_luong is null and thang_tldg_dt_dai is null  
				   and manv_tt_dai is not null and loaitb_id=20
				   and (nop_du=1 or mien_hsgoc is not null) 
--				   and not exists (select 1 from ttkd_bsc.nhanvien where thang = 202406 and loai_ld like '_LCN' and ma_nv = a.manv_tt_dai)
			;
			--33

-- PGP
--			update ttkd_bsc.ct_bsc_ptm 
--			    set thang_tldg_dt_nvhotro=null
--			    where thang_ptm =202402 		---thang n-2
--						and thang_tldg_dt_nvhotro=202404	---thang n
--						and (loaitb_id not in (21,131) or ma_kh='GTGT rieng')
--						  and manv_hotro is not null 
				;
		--34
    
			update ttkd_bsc.ct_bsc_ptm a 
			    set thang_tldg_dt_nvhotro = 202406		---thang n
--		select thang_luong, thang_ptm, thang_tldg_dt, thang_tldg_dt_nvhotro, luong_dongia_nvptm, luong_dongia_nvhotro, lydo_khongtinh_dongia from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm = 202404 --thang n-2
					and (loaitb_id not in (210, 21,131) or ma_kh='GTGT rieng') and (chuquan_id is null or chuquan_id<>264)
					
					and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
					 and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%')		---PGP dc tinh khi ban qua dai ly
										)
					and ( (thang_tldg_dt is null and lydo_khongtinh_luong like '%Phat trien qua Dai ly%')			---PGP dc tinh khi ban qua dai ly
										or thang_tldg_dt = 202406				---tam thoi PTM < T5, sau do chuyen ve is not null sau >= T5
							)
					  and nvl(thang_tldg_dt_nvhotro, 999999) >= 202406 
--					  and thang_luong = 11
					  and ( trangthaitb_id_n2=1 or (loaitb_id=20 and trangthaitb_id_n2=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146))) or loaitb_id in (89,90,146) )
					  and nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)+nvl(dthu_ps_n2,0)>0 and nocuoc_n2 is null  
					  and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
					  and (nop_du=1 or mien_hsgoc is not null) 
--					  and exists(select 1 from ttkd_bsc.nhanvien where thang = 202405 and ma_pb='VNP0702600' and ma_nv=a.manv_hotro)
					
			;
            ---35
commit;
--*************** ky n-3 *********
--			update ttkd_bsc.ct_bsc_ptm a 
--			    set thang_tldg_dt=''
--			    where thang_ptm=202401 ---thang n-3
--							and thang_tldg_dt=202404	--thang n
--							and (loaitb_id<>21 or ma_kh='GTGT rieng')
			    ;
		--36

-- Dot 1: dot 2 khong chay
			update ttkd_bsc.ct_bsc_ptm a 
			    set thang_tldg_dt = 202406			---thang n
			   -- select thang_ptm, dich_vu, ma_tb, ma_to, loai_ld, trangthaitb_id_n3, nocuoc_n3, ngay_luuhs_ttkd,nop_du, thang_tldg_dt, heso_hoso from ttkd_bsc.ct_bsc_ptm a 
			    where thang_ptm = 202403 ---thang n-3
					and (loaitb_id not in (21,131) or ma_kh='GTGT rieng') and chuquan_id in (145,266) 
					  and lydo_khongtinh_luong is null and thang_tldg_dt is null 
					  and ( trangthaitb_id_n3=1 or (loaitb_id=20 and trangthaitb_id_n3=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146)))or loaitb_id in (89,90,146) )
				
					    and (nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)+nvl(dthu_ps_n2,0)+nvl(dthu_ps_n3,0)>0 
									or (nvl(dthu_ps_n2, 0) =0 and loaitb_id in (55,80,116,117,140,132,122,288,181,290,292,175,302) 
													and dthu_goi >0 and thang_bddc > 202406 --thang n
										)
							) 
					and nocuoc_n3 is null
					  and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
					  and (nop_du=1 or mien_hsgoc is not null) 
					  and (loai_ld not like '_LCN' or loai_ld is null)
			;
		--37 
		
--			update ttkd_bsc.ct_bsc_ptm a set thang_tldg_dnhm=null
--			    where thang_ptm=202401 ---thang n-3
--							and thang_tldg_dnhm=202404		---thang n
			    ;
        --38
	   
			update ttkd_bsc.ct_bsc_ptm a 
				set thang_tldg_dnhm = 202406	--thang n
			    -- select chuquan_id, ma_tb, manv_ptm, ten_pb, thang_tldg_dt,  thang_tldg_dnhm, tien_dnhm, tien_tt, thang_ptm,  to_char(ngay_tt,'yyyymm'), ngay_tt, soseri, trangthai_tt_id from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm = 202403	--thang n-3
						and thang_tldg_dt = 202406		---thang n
						and (loaitb_id not in (21,210, 222) or ma_kh='GTGT rieng')   
						 and thang_tldg_dnhm is null              
						-- and lydo_khongtinh_luong is null 
						 and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%' and heso_daily=0.05 and MA_NGUOIGT is not null)
								)
						 and chuquan_id in (145,266,264)     
						 and (tien_dnhm>0 or tien_sodep>0)
						 and ((loaitb_id not in (20,149)  and ngay_tt is not null and trangthai_tt_id=1 ) or loaitb_id in (20,149))
						 and (loai_ld not like '_LCN' or loai_ld is null)         
						 and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
						 ;
                --39       dot 2 khong chay        

-- Dot 2: 
				update ttkd_bsc.ct_bsc_ptm a 
					   set thang_tldg_dt = 202406		--thang n
--				   select  chuquan_id, thang_ptm, dich_vu, ma_pb, ma_tb, ma_to, loai_ld, trangthaitb_id_n3, nocuoc_n3, dthu_ps, dthu_ps_n1,dthu_ps_n2, dthu_ps_n3, ngay_luuhs_ttkd,nop_du, thang_tldg_dt, heso_hoso, lydo_khongtinh_dongia from ttkd_bsc.ct_bsc_ptm a 
				    where thang_ptm = 202403 		---thang n-3
						  and (loaitb_id<>21 or ma_kh='GTGT rieng') and (chuquan_id in  (145,266) or chuquan_id is null)
						   and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%' and heso_daily=0.05 and MA_NGUOIGT is not null)
							) 
--						   and thang_tldg_dt is null 
--							and nvl(thang_tldg_dt, 0) <>  999999			---cac th bi khoa lai
							and nvl(thang_tldg_dt, 999999) >= 202406
						   and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
						  and (trangthaitb_id_n3=1 or (loaitb_id=20 and trangthaitb_id_n3=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146)))or loaitb_id in (89,90,146) )												  
						    and (nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)+nvl(dthu_ps_n2,0)+nvl(dthu_ps_n3,0)>0 
									or (nvl(dthu_ps_n2, 0) =0 and loaitb_id in (55,80,116,117,140,132,122,288,181,290,292,175,302) 
													and dthu_goi >0 and thang_bddc > 202406		--thang n
										)
							) 
							and nocuoc_n3 is null					
						  and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
						  and (nop_du=1 or mien_hsgoc is not null) 
						  and (loai_ld not like '_LCN' or loai_ld is null)
						--  and manv_ptm is not null	
						
					;
		--40

				update ttkd_bsc.ct_bsc_ptm a 
						set thang_tldg_dnhm = 202406		---thang n
--				    select chuquan_id, ma_tb, manv_ptm, ten_pb, thang_tldg_dt,  thang_tldg_dnhm, tien_dnhm, tien_tt, thang_ptm,  to_char(ngay_tt,'yyyymm'), ngay_tt, soseri, trangthai_tt_id from ttkd_bsc.ct_bsc_ptm a
				    where thang_ptm = 202403 		---thang n-3
							and thang_tldg_dt = 202406      	---thang n
							and (loaitb_id not in (21,210, 222) or ma_kh='GTGT rieng')  and chuquan_id in (145,266,264) 
							 and nvl(thang_tldg_dnhm, 999999) >=202406        
					
							and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%' and heso_daily=0.05 and MA_NGUOIGT is not null)
								)
							 and (tien_dnhm>0 or tien_sodep>0)
							 and ((loaitb_id not in (20,149)  and ngay_tt is not null and trangthai_tt_id=1 ) or loaitb_id in (20,149))
							 and (loai_ld not like '_LCN' or loai_ld is null)         
							 and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
				;


-- VNPTS cua BHOL
				update ttkd_bsc.ct_bsc_ptm a 
					   set thang_tldg_dt_dai = 202406		---thang n
--				   select thang_tldg_dt, thang_tldg_dt_dai from ttkd_bsc.ct_bsc_ptm a 
				    where thang_ptm = 202403 ---thang n-3
							 and thang_tldg_dt = 202406
							 and nvl(thang_tldg_dt_dai, 999999) >= 202406
							   and (loaitb_id<>21 or ma_kh='GTGT rieng') and chuquan_id in (145,266,264) 
							   and lydo_khongtinh_luong is null     
							   and ( trangthaitb_id_n3=1 or (loaitb_id=20 and trangthaitb_id_n3=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146)))or loaitb_id in (89,90,146) )
							   and nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)+nvl(dthu_ps_n2,0)+nvl(dthu_ps_n3,0)>0 and nocuoc_n3 is null
							   and (nop_du=1 or mien_hsgoc is not null) 
--							   and manv_tt_dai is not null
--							   and not exists (select 1 from ttkd_bsc.nhanvien where thang = 202405 and loai_ld like '_LCN' and ma_nv =a.manv_tt_dai)
				;


-- PGP
			update ttkd_bsc.ct_bsc_ptm a 
			    set thang_tldg_dt_nvhotro = 202406		---thang n
--			    select thang_ptm, thang_tldg_dt, thang_tldg_dt_nvhotro, lydo_khongtinh_dongia, luong_dongia_nvptm, luong_dongia_nvhotro, manv_hotro from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm = 202403 	---thang n-3
							and nvl(THANG_TLDG_DT_NVHOTRO, 999999) >= 202406
							and (loaitb_id not in (21,131) or ma_kh='GTGT rieng') and chuquan_id in (145,266) 
							 and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%')		---PGP dc tinh khi ban qua dai ly
												) 
							and ( (thang_tldg_dt is null and lydo_khongtinh_luong like '%Phat trien qua Dai ly%')			---PGP dc tinh khi ban qua dai ly
												or thang_tldg_dt = 202406				---tam thoi PTM < T5, sau do chuyen ve is not null sau >= T5
									)
							  and ( trangthaitb_id_n3=1 or (loaitb_id=20 and trangthaitb_id_n3=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146))) or loaitb_id in (89,90,146) )
							  and nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)+nvl(dthu_ps_n2,0)+nvl(dthu_ps_n3,0)>0 and nocuoc_n3 is null
							  and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
							  and (nop_du=1 or mien_hsgoc is not null) 
--					  and exists(select 1 from ttkd_bsc.nhanvien 
--										where thang = 202405 and ma_pb='VNP0702600' and ma_nv=a.manv_hotro)
			--					  and thang_luong = 11
--			 and thang_luong= 21
			;
            
 commit; 
-- GTGT rieng thang n
			update ttkd_bsc.ct_bsc_ptm a 
				   set  thang_tldg_dt = 202406, thang_tlkpi= 202406, thang_tlkpi_to =202406, thang_tlkpi_phong =202406,
						 thang_tldg_dt_nvhotro=case when manv_hotro is not null and thang_tldg_dt_nvhotro is null then 202406 end
--			     select * from ttkd_bsc.ct_bsc_ptm a 
			    where thang_ptm >= 202403 ---thang n-3
								and lydo_khongtinh_luong is null and thang_xacnhan_khkt is not null 
							   and thang_tldg_dt is null and ma_kh='GTGT rieng'
			;
			
----Quyet tinh cac so con 30B+D theo so chu (thuebao_cha_id duoc tinh)
		
		update ttkd_bsc.ct_bsc_ptm a
				set thang_tldg_dt = (case when exists (select 1 from ttkd_bsc.ct_bsc_ptm where thuebao_id = a.thuebao_cha_id and thang_tldg_dt is null)
																	then null
															else a.thang_tldg_dt
												end)
--		select thuebao_id, thuebao_cha_id, case when exists (select 1 from ttkd_bsc.ct_bsc_ptm where thuebao_id = a.thuebao_cha_id and thang_tldg_dt is null)
--																			then null
--																else thang_tldg_dt
--															end kq_thuebao_cha_id, thang_tldg_dt, lydo_khongtinh_dongia
--		from ttkd_bsc.ct_bsc_ptm a
		where (thuebao_cha_id is not null and loaitb_id = 17)		---bsung 22/6
										and thang_ptm >= 202403					---thang n-3
										and nvl(thang_tldg_dt, 999999) >= 202406			--thang n
;
   
        
-- Tinh bs ho so gia han CA, IVAN,... dthu ps =0 nhung chua den ky dat coc moi: 
	---khong chay thang 5/2025, KIEMTRA xem da duoc tinh khong?
			update ttkd_bsc.ct_bsc_ptm a 
				   set thang_tldg_dt = 202406, thang_tlkpi = 202406, thang_tlkpi_to=202406, thang_tlkpi_phong=202406
						, thang_tldg_dt_nvhotro = 202406
						--, thang_luong = 87
--			     select thang_luong, thang_ptm, thang_tldg_dt, thang_tldg_dt_nvhotro, ma_gd, ma_tb, dthu_goi, datcoc_csd, thang_bddc, doanhthu_dongia_nvptm, heso_daily, lydo_khongtinh_luong, lydo_khongtinh_dongia, dthu_ps from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm >= 202403 		---thang n-3
						and thang_bddc >= 202406			---thang n
							--and lydo_khongtinh_luong is null 
							 and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%' and heso_daily=0.05 and MA_NGUOIGT is not null)
									)
							and nvl(thang_tldg_dt, 999999) >= 202406
						   and loaitb_id in (55,80,116,117,140,132,122,288,181,290,292,175,302) 
			--	   and ma_tb = 'hcm_ca_00067906'
			;
			rollback;
			commit;
                
---- PGP - thieu hsg thang n
--				update ttkd_bsc.ct_bsc_ptm a 
--				    set  thang_tldg_dt_nvhotro=202404, thang_tlkpi_hotro=202404
--				    -- select * from ttkd_bsc.ct_bsc_ptm_pgp
--				    where thang_ptm>=202401		---thang n-3
--					   and lydo_khongtinh_dongia='; Chua nop du ho so'
--					   and thang_tldg_dt_nvhotro is null
--					   ;
--        
        
---- SMS Brn dot 1 ngay 5: khong chay dot 2 do chua co cuoc ps thang n+1
--				update ttkd_bsc.ct_bsc_ptm a 
--				    set thang_tldg_dt='', thang_tlkpi='', thang_tlkpi_to='', thang_tlkpi_phong=''
--			--- select * from ttkd_bsc.ct_bsc_ptm a 
--				    where loaitb_id in(131,358) 
--								and thang_ptm=202403		---thang n-1
--								and thang_tldg_dt=202404 		---thang n
--				;
--
commit;
  ----kiem tien dau noi hoa mang---
select thang_ptm, ma_gd, ma_tb, soseri, ngay_tt, tien_tt, tien_dnhm, trangthai_tt_id, chuquan_id, manv_ptm, MA_NGUOIGT
			, lydo_khongtinh_luong, heso_daily
  from ttkd_bsc.ct_bsc_ptm a
  where thang_tldg_dt=202406 and trangthai_tt_id=1 and (tien_dnhm>0 or tien_sodep >0) and ngay_tt is not null
    and exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id)
    and thang_tldg_dnhm is null
    ;
    ----Kiem tra lydo_khongtinh_XXX co gia tri
    select thang_ptm, ma_gd, ma_tb, soseri, ngay_tt, tien_tt, tien_dnhm, trangthai_tt_id, chuquan_id, manv_ptm, MA_NGUOIGT
			, thang_bddc, lydo_khongtinh_luong, lydo_khongtinh_dongia, heso_daily, nop_du
  from ttkd_bsc.ct_bsc_ptm a
  where thang_tldg_dt=202406
					and (lydo_khongtinh_luong is not null or lydo_khongtinh_dongia is not null )
	;
	   ----Kiem tra lydo_khongtinh_XXX khong co gia tri
    select thang_ptm, ma_gd, nguon, ma_tb, thuebao_id, hdtb_id, dich_vu, trangthai_tt_id, chuquan_id, manv_ptm, ten_pb
			, thang_bddc, lydo_khongtinh_luong, lydo_khongtinh_dongia, heso_daily, nop_du, mien_hsgoc, trangthaitb_id, dthu_goi
			, THANG_TLDG_DT, THANG_TLKPI, THANG_TLKPI_TO, THANG_TLKPI_PHONG
  from ttkd_bsc.ct_bsc_ptm a
  where thang_tldg_dt is null and thang_ptm >= 202402
					and loaitb_id not in (21)
					and  lydo_khongtinh_dongia is null and manv_ptm is not null 
					and loai_ld not like '%LCN'		--dai ly ca nhan khong tinh don gia, chi tinh KPI
	;
	----Kiem tra du lieu imp duoc tinh hay chua
	  select 
				thang_ptm, ma_gd, ma_tb, soseri, ngay_tt, tien_tt, tien_dnhm, trangthai_tt_id, chuquan_id, manv_ptm, MA_NGUOIGT
			, thang_bddc, lydo_khongtinh_luong, lydo_khongtinh_dongia, heso_daily, nop_du
			, heso_hotro_nvhotro, thang_tldg_dt_nvhotro, thang_tldg_dt, dthu_goi, luong_dongia_nvptm, dthu_ps
  from ttkd_bsc.ct_bsc_ptm a
  where thang_luong in (87);
  
  ----Kiem tra cac truong hop có FLAG khong tinh luong
   select thang_luong, thang_ptm, ma_gd, nguon, ma_tb, thuebao_id, hdtb_id, dich_vu, trangthai_tt_id, chuquan_id, manv_ptm, ten_pb
			, thang_bddc, lydo_khongtinh_luong, lydo_khongtinh_dongia, heso_daily, nop_du, mien_hsgoc, trangthaitb_id, dthu_goi
			, THANG_TLDG_DT, THANG_TLKPI, THANG_TLKPI_TO, THANG_TLKPI_PHONG
  from ttkd_bsc.ct_bsc_ptm a
  where  thang_luong < 100
					and loaitb_id not in (21)
					and loai_ld not like '%LCN'		--dai ly ca nhan khong tinh don gia, chi tinh KPI
	;
  

