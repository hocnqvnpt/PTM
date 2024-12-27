select * from ttkd_bsc.v_ct_ptm_kpi_truongline_202411;
drop view ttkd_bsc.v_ct_ptm_kpi_truongline_202411;

CREATE OR REPLACE VIEW ttkd_bsc.v_ct_ptm_kpi_truongline_202411 as
select ma_pb, ma_to, ten_to, loaitb_id, dichvuvt_id, sum(doanhthu_kpi_to) dthu_kpi, nguon
			from(
					---- To NVPTM
					select ma_pb, ma_to, ten_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_to * heso_hotro_nvptm as doanhthu_kpi_to, cast('vb353_ptm' as varchar(20)) nguon
					  from ttkd_bsc.ct_bsc_ptm a
					  where thang_tlkpi_to = 202411 and (loaitb_id<>21 or loaitb_id is null) 
									and doanhthu_kpi_to >0
				union all
					---- To NVHOTRO DIGISHOP
					select b.ma_pb, b.ma_to, b.ten_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_to * heso_hotro_nvhotro as doanhthu_kpi_to, 'vb353_ptm' nguon
					  from ttkd_bsc.ct_bsc_ptm a
									join ttkd_bsc.nhanvien b on a.thang_tlkpi_to = b.thang and a.manv_hotro = b.ma_nv
					  where thang_tlkpi_to = 202411 and (loaitb_id<>21 or loaitb_id is null)
									and tyle_am is null and tyle_hotro is null 
									and doanhthu_kpi_to >0
									and nvl(vanban_id, 0) != 764  ---only T7, 8,9, thang sau xoa
				union all
					---To  Nvien Cot MANV_HOTRO PGP
					select b.ma_pb, b.ma_to, b.ten_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_nvhotro, 'vb353_ptm' nguon
					  from ttkd_bsc.ct_bsc_ptm a
									join ttkd_bsc.nhanvien b on a.thang_tlkpi_hotro = b.thang and a.manv_hotro = b.ma_nv
					 where thang_tlkpi_hotro = 202411 and (loaitb_id<>21 or loaitb_id is null)
									and tyle_am is not null and tyle_hotro is not null 
									and doanhthu_kpi_nvhotro >0
--				 ---MANV_DAI
					union all
						select b.ma_pb, b.ma_to, b.ten_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_to * heso_hotro_dai, 'vb353_ptm' nguon
						from ttkd_bsc.ct_bsc_ptm a
										join ttkd_bsc.nhanvien b on a.thang_tlkpi_to = b.thang and a.manv_tt_dai = b.ma_nv
						where thang_tlkpi_to = 202411 and (loaitb_id<>21 or ma_kh='GTGT rieng')
									and doanhthu_kpi_to >0
									and nvl(vanban_id, 0) != 764  ---only T7, 8,9, thang sau xoa
				union all
					---To  Nvien Cot MANV_PTM cho dthu DNHM
					select ma_pb, ma_to, ten_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_dnhm_phong * heso_hotro_nvptm as doanhthu_kpi_dnhm, 'vb353_ptm' nguon
					  from ttkd_bsc.ct_bsc_ptm a
					 where thang_tlkpi_dnhm_to = 202411 and (loaitb_id<>21 or loaitb_id is null)
									and doanhthu_kpi_dnhm_phong >0
				union all
					---To  Nvien Cot MANV_HOTRO cho dthu DNHM tren DIGISHOP
					select b.ma_pb, b.ma_to, b.ten_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_dnhm_phong * heso_hotro_nvhotro as doanhthu_kpi_dnhm, 'vb353_ptm' nguon
					  from ttkd_bsc.ct_bsc_ptm a
								join ttkd_bsc.nhanvien b on a.thang_tlkpi_dnhm_to = b.thang and a.manv_hotro = b.ma_nv
					 where thang_tlkpi_dnhm_to = 202411 and (loaitb_id<>21 or loaitb_id is null)
									and tyle_am is null and tyle_hotro is null
									and doanhthu_kpi_dnhm_phong >0
									and nvl(vanban_id, 0) != 764 ---only T7,8,9, thang sau xoa
--				union all ---dthu DNHM PGP không có tinh Tổ trưởng
				union all
					select a.ma_pb, a.ma_to, ten_to, ma_tb, 20 loaitb_id, 2 dichvuvt_id, doanhthu_dongia, 'vnpts_ptm_ghtt' nguon
					from ttkd_bsc.ghtt_vnpts	a	----a Nguyen quan ly
									left join ttkd_bsc.dm_to b on a.ma_to = b.ma_to
					  where thang=202411 and thang_giao is not null and a.ma_to is not null 
				
				union all
					----GHTT toBHOL BHKV (Nhu Y)
						select a.ma_pb, a.ma_to, nv.ten_to, 'ghtt' ma_tb, 58, 4, sum(TIEN) dthu_kpi, 'brcd_qd' nguon
						from ttkd_Bsc.tl_Giahan_Tratruoc a
									join ttkd_bsc.nhanvien nv on a.thang = nv.thang and a.ma_nv = nv.ma_nv
						where a.thang = 202411 and loai_tinh = 'DOANHTHU'
--									and nv.ma_vtcv not in ('VNP-HNHCM_BHKV_52', 'VNP-HNHCM_BHKV_53')
									and a.ma_to in ('VNP0703004') --- to OBBH - PBHOL
						group by a.ma_pb, a.ma_to, nv.ten_to
				----VB VNP hien huu 384 (Viet Anh)
					union all
						select ma_pb, ma_to, ten_to, ma_tb, decode(loaihinh_tb, 'TT', 21, 20), 2, dthu_kpi, 'vnp_qd' nguon
--						select sum(dthu_kpi)
						from vietanhvh.dongia_DTHH
						where thang = 202411 
									and (ma_vtcv in ('VNP-HNHCM_BHKV_52', 'VNP-HNHCM_BHKV_53') ---To BHOL BHKV
												or (ma_to in ('VNP0703004') and ma_vtcv in ('VNP-HNHCM_KDOL_17')) --- NV OBBH - To OBBH - PBHOL
												)
--									and ma_vtcv in ('VNP-HNHCM_KDOL_17') --- NV OBBH - To OBBH - PBHOL
					  
						) group by ma_pb, ma_to, ten_to, loaitb_id, dichvuvt_id, nguon