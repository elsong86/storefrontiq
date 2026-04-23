import { useRouter } from 'next/navigation';
import { Location } from '../types';

export function handleLocationShare(
  loc: Location,
  router: ReturnType<typeof useRouter>,
) {
  console.log('Location shared:', loc);
  router.push(`/search?latitude=${loc.latitude}&longitude=${loc.longitude}`);
}
