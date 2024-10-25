select a.ma_nv, ten_nv, ten_pb, a.luong_dongia_ghtt, b.tien_ob, b.tien_ts
from ttkd_bsc.bangluong_dongia_202405_l4 a 
        join (select ma_nv, sum(case when loai_tinh in ('DONGIATRA_OB') then tien else 0 end) tien_ob
                  , sum(case when loai_tinh in ('DONGIA_TS_TP_TT') then tien else 0 end) tien_ts
                        from ttkd_bsc.temp_ghtt
                        where ma_kpi = 'DONGIA' and loai_tinh in ('DONGIATRA_OB', 'DONGIA_TS_TP_TT')
                          group by ma_nv having  sum(tien) <> 0) b on a.ma_nv = b.ma_nv
--    where a.luong_dongia_ghtt - b.tien_ob <> b.tien_ts;

