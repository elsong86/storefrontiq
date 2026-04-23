import Cookies from 'js-cookie';

export function trackVisit() {
  let visits = Cookies.get('visits');

  let visitCount = parseInt(visits || '0', 10);

  visitCount++;

  Cookies.set('visits', visitCount.toString(), { expires: 365 });
}
